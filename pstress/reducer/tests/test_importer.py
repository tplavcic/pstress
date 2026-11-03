"""Milestone A: importer and manifest tests (no database needed)."""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from reducerlib.importer import (import_plain_sql, import_pstress_logs, parse_thread_log)  # noqa: E402
from reducerlib.manifest import Candidate, Workload, validate_manifest  # noqa: E402
from reducerlib.util import ReducerError  # noqa: E402

TS = "2026-10-21T12:24:10 3003809=>475ms "
NODE = "default.node.tld"


def write(dirp: Path, name: str, data, mode="w"):
    p = dirp / name
    p.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, bytes):
        p.write_bytes(data)
    else:
        p.write_text(data)
    return p


def imp(d, **kw):
    args = dict(step=None, node=None, setup_spec="none", database="test", create_databases=None,
                loader="mysql-cli", snapshot_checksum="content", external_files=[], include_failed=True,
                include_incomplete=False, skip_unframed=False, allow_empty_setup=False, pstress_revision=None,
                autocommit="server-default")
    args.update(kw)
    return import_pstress_logs(Path(d), **args)


class ParseTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d)

    def parse(self, text, **kw):
        p = write(self.d, "x.sql", text) if isinstance(text, str) else write(self.d, "x.sql", text)
        return parse_thread_log(p, **kw)

    def test_success_and_failure_records_without_duration(self):
        r = self.parse(" S USE test rows:0\n S SELECT 1 rows:1\n F INSERT INTO t VALUES (1)\n"
                       "Error Duplicate entry '1' for key 't.PRIMARY'\n")
        self.assertEqual([s["sql"] for s in r.statements], ["USE test", "SELECT 1", "INSERT INTO t VALUES (1)"])
        self.assertEqual(r.statements[1]["orig"]["rows"], 1)
        self.assertEqual(r.statements[2]["orig"]["status"], "F")
        self.assertEqual(r.statements[2]["orig"]["error"], "Duplicate entry '1' for key 't.PRIMARY'")
        self.assertEqual(r.counts["S"], 2)
        self.assertEqual(r.counts["F"], 1)

    def test_duration_prefix_and_unit_quirk(self):
        r = self.parse(TS + " S SELECT 2 rows:1\n", duration_unit="us")
        st = r.statements[0]
        self.assertEqual(st["sql"], "SELECT 2")
        self.assertEqual(st["orig"]["timing"], {"wallclock": "2026-10-21T12:24:10", "start_raw": 3003809,
                                                "duration_raw": 475, "logged_unit": "ms", "actual_unit": "us"})

    def test_orphan_timing_prefixes(self):
        # --log-query-duration without logging the previous result leaves its prefix in front
        r = self.parse(TS + "2026-10-21T12:24:11 5=>6ms  S SELECT 3 rows:1\n")
        self.assertEqual(r.statements[0]["sql"], "SELECT 3")
        self.assertEqual(r.statements[0]["orig"]["timing"]["start_raw"], 5)
        self.assertEqual(r.counts["orphan_timing_prefixes"], 1)

    def test_metadata_like_text_inside_sql_is_preserved(self):
        sql = "INSERT INTO t VALUES ('x rows:5 y', ' S ', ' F ', 'rows:7')"
        r = self.parse(f" S {sql} rows:1\n F {sql} rows:2\nError boom\n")
        self.assertEqual(r.statements[0]["sql"], sql)
        self.assertEqual(r.statements[1]["sql"], sql + " rows:2")  # F records have no rows field
        self.assertEqual(r.statements[0]["orig"]["rows"], 1)

    def test_sql_ending_with_rows_text_strips_exactly_one_suffix(self):
        r = self.parse(" S SELECT 'a' AS ` rows:3` rows:1\n")
        self.assertEqual(r.statements[0]["sql"], "SELECT 'a' AS ` rows:3`")

    def test_quotes_comments_semicolons(self):
        sql = "SELECT ';' AS a, \"x;y\" /* c; */, `id;` FROM t -- trailing; comment"
        r = self.parse(f" S {sql} rows:0\n")
        self.assertEqual(r.statements[0]["sql"], sql)

    def test_duplicates_preserved_in_order(self):
        r = self.parse(" S SELECT 1 rows:1\n S SELECT 1 rows:1\n F SELECT 1\nError x\n S SELECT 1 rows:1\n")
        self.assertEqual(len(r.statements), 4)
        self.assertEqual([s["orig"]["status"] for s in r.statements], ["S", "S", "F", "S"])

    def test_multiline_success_statement(self):
        r = self.parse(" S SELECT 'a\nb\n' AS x rows:1\n S SELECT 2 rows:1\n")
        self.assertEqual(r.statements[0]["sql"], "SELECT 'a\nb\n' AS x")
        self.assertEqual(r.statements[0]["source"], {"file": "x.sql", "line_start": 1, "line_end": 3})
        self.assertEqual(r.statements[1]["sql"], "SELECT 2")

    def test_multiline_failure_and_multiline_error(self):
        r = self.parse(" F SELEC\n1\nError You have an error in your SQL syntax; check the manual near 'SELEC\n"
                       "1' at line 1\n S SELECT 2 rows:1\n")
        self.assertEqual(r.statements[0]["sql"], "SELEC\n1")
        self.assertIn("1' at line 1", r.statements[0]["orig"]["error"])
        self.assertEqual(r.statements[1]["sql"], "SELECT 2")
        self.assertTrue(any("multi-line error" in w["message"] for w in r.warnings))

    def test_server_gone_diagnostic_is_deduplicated(self):
        r = self.parse(" S SELECT 1 rows:1\n F ALTER TABLE t ADD COLUMN c INT\n"
                       "Error Lost connection to MySQL server during query\n"
                       "server gone, while processing ALTER TABLE t ADD COLUMN c INT\n")
        self.assertEqual(len(r.statements), 2)
        self.assertTrue(r.statements[1]["orig"]["server_gone"])
        self.assertEqual(r.counts["server_gone_dedup"], 1)

    def test_server_gone_without_failure_record_is_imported_labelled(self):
        r = self.parse(" S SELECT 1 rows:1\nserver gone, while processing DELETE FROM t\n")
        self.assertEqual(r.statements[1]["sql"], "DELETE FROM t")
        self.assertEqual(r.statements[1]["orig"]["status"], "LOST")
        self.assertTrue(r.statements[1]["orig"]["from_diagnostic"])

    def test_incomplete_final_record(self):
        r = self.parse(b" S SELECT 1 rows:1\n S SELECT 'trunc")
        self.assertEqual(len(r.statements), 1)
        self.assertEqual(len(r.incomplete), 1)
        self.assertEqual(r.incomplete[0]["line_start"], 2)
        self.assertTrue(any("newline" in w["message"] for w in r.warnings))

    def test_unframed_record_reported(self):
        r = self.parse(" S SELECT 1\n S SELECT 2 rows:1\n")
        self.assertEqual(len(r.unframed), 1)
        self.assertEqual(r.unframed[0]["line_start"], 1)
        self.assertEqual([s["sql"] for s in r.statements], ["SELECT 2"])

    def test_diagnostics_and_unrecognized_lines(self):
        r = self.parse("waiting for metadata load to finish\nInitial seed 43\nCURRENT SEED IS 20355\n"
                       " value of rand_int(100) 31\nthreads=>3\ninsert random row, total=>5, success=> 4, skipped=> 0\n"
                       "something odd\n S SELECT 1 rows:1\n")
        self.assertEqual(len(r.statements), 1)
        self.assertEqual(len(r.unrecognized), 1)
        self.assertEqual(r.unrecognized[0]["line"], 7)
        self.assertEqual(sum(r.diagnostics.values()), 6)

    def test_non_utf8_bytes_round_trip(self):
        raw = b" S INSERT INTO t VALUES ('\xff\xfe') rows:1\n"
        r = self.parse(raw)
        sql = r.statements[0]["sql"]
        self.assertEqual(sql.encode("utf-8", "surrogateescape"), b"INSERT INTO t VALUES ('\xff\xfe')")
        again = json.loads(json.dumps(sql))
        self.assertEqual(again.encode("utf-8", "surrogateescape"), b"INSERT INTO t VALUES ('\xff\xfe')")


    def test_indented_diagnostic_text_is_not_unrecognized(self):
        r = self.parse("fulltext search, total=>3, success=> 3, skipped=> 0\n"
                       "    MATCH ... AGAINST on a table with a FULLTEXT index: natural language,\n"
                       "    boolean modes.\n S SELECT 1 rows:1\n")
        self.assertEqual(r.unrecognized, [])
        self.assertEqual(len(r.statements), 1)


class KeyringTests(unittest.TestCase):
    """pstress-run writes <datadir>/component_keyring_file.cnf with an absolute path."""

    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d)

    def make_datadir(self, path, keyring_inside=True):
        path.mkdir(parents=True)
        (path / "mysql.ibd").write_bytes(b"x")
        if keyring_inside:
            (path / "component_keyring_file").write_bytes(b"k")
        (path / "component_keyring_file.cnf").write_text(json.dumps(
            {"path": "/gone/rundir/333197/1/data/component_keyring_file", "read_only": False}))
        return path

    def test_attempt_copy_is_repointed(self):
        from reducerlib.lifecycle import relocate_keyring_configs
        dd = self.make_datadir(self.d / "attempt" / "data")
        notes = relocate_keyring_configs(dd)
        cfg = json.loads((dd / "component_keyring_file.cnf").read_text())
        self.assertEqual(cfg["path"], str(dd / "component_keyring_file"))
        self.assertFalse(cfg["read_only"])
        self.assertEqual(len(notes), 1)
        self.assertEqual(relocate_keyring_configs(dd), [])  # idempotent

    def test_keyring_outside_snapshot_left_alone(self):
        from reducerlib.lifecycle import relocate_keyring_configs
        dd = self.make_datadir(self.d / "data", keyring_inside=False)
        notes = relocate_keyring_configs(dd)
        self.assertIn("not inside the snapshot", notes[0])
        self.assertEqual(json.loads((dd / "component_keyring_file.cnf").read_text())["path"],
                         "/gone/rundir/333197/1/data/component_keyring_file")

    def test_import_records_keyring(self):
        from reducerlib.importer import build_setup
        dd = self.make_datadir(self.d / "1" / "data")
        setup = build_setup(f"snapshot:{dd}", create_databases=["test"], loader="mysql-cli",
                            snapshot_checksum="metadata", external_files=[])
        self.assertEqual(setup["keyring"], [{"config": "component_keyring_file.cnf",
                                             "path": "/gone/rundir/333197/1/data/component_keyring_file",
                                             "file_in_snapshot": True}])


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.d)

    def test_ambiguous_steps_require_selection(self):
        write(self.d, f"1/{NODE}_step_1_thread-0.sql", " S SELECT 1 rows:1\n")
        write(self.d, f"2/{NODE}_step_2_thread-0.sql", " S SELECT 2 rows:1\n")
        with self.assertRaises(ReducerError) as cm:
            imp(self.d)
        self.assertIn("several steps", str(cm.exception))
        m, rep = imp(self.d, step=1)
        self.assertEqual(m["source"]["step"], 1)
        self.assertEqual(m["sessions"][0]["statements"][0]["sql"], "SELECT 1")

    def test_later_step_requires_explicit_setup(self):
        write(self.d, f"2/{NODE}_step_2_thread-0.sql", " S SELECT 2 rows:1\n")
        (self.d / "1" / "data").mkdir(parents=True)
        with self.assertRaises(ReducerError) as cm:
            imp(self.d, step=2)
        self.assertIn("snapshot", str(cm.exception))
        m, _ = imp(self.d, step=2, allow_empty_setup=True)
        self.assertTrue(any("EMPTY setup" in x for x in m["capture"]["limitations"]))
        self.assertFalse(m["capture"]["complete"])

    def test_multi_node_rejected_unless_node_selected(self):
        write(self.d, "node1_step_1_thread-0.sql", " S SELECT 1 rows:1\n")
        write(self.d, "node2_step_1_thread-0.sql", " S SELECT 2 rows:1\n")
        with self.assertRaises(ReducerError) as cm:
            imp(self.d)
        self.assertIn("multi-node", str(cm.exception))
        m, rep = imp(self.d, node="node2")
        self.assertEqual(len(m["sessions"]), 1)
        self.assertTrue(any("excluded" in n for n in rep["notes"]))

    def test_unframed_requires_correction(self):
        write(self.d, f"{NODE}_step_1_thread-0.sql", " S SELECT 1\n S SELECT 2 rows:1\n")
        with self.assertRaises(ReducerError) as cm:
            imp(self.d)
        self.assertEqual(cm.exception.exit_code, 7)
        m, rep = imp(self.d, skip_unframed=True)
        self.assertEqual(len(rep["excluded_statements"]), 1)

    def test_session_identity_and_ids(self):
        write(self.d, f"{NODE}_step_1_thread-0.sql", " S USE db1 rows:0\n S SELECT 1 rows:1\n")
        write(self.d, f"{NODE}_step_1_thread-3.sql", " S SELECT 3 rows:1\n")
        m, _ = imp(self.d)
        ids = [s["id"] for s in m["sessions"]]
        self.assertEqual(ids, [f"{NODE}:step1:thread-0", f"{NODE}:step1:thread-3"])
        self.assertEqual(m["sessions"][0]["database"], "db1")
        self.assertEqual(m["sessions"][1]["database"], "test")
        self.assertEqual(m["setup"]["create_databases"], ["db1", "test"])
        self.assertEqual(m["sessions"][0]["statements"][1]["id"], "t0.000002")
        validate_manifest(m)

    def test_exclude_failed_is_recorded(self):
        write(self.d, f"{NODE}_step_1_thread-0.sql", " S SELECT 1 rows:1\n F SELECT x\nError e\n")
        m, rep = imp(self.d, include_failed=False)
        self.assertEqual(len(m["sessions"][0]["statements"]), 1)
        self.assertEqual(len(rep["excluded_statements"]), 1)


class RealFixtureTests(unittest.TestCase):
    """Logs written by pstress cf59749 against MySQL 8.0.46 (truncated to 120 lines each)."""

    def test_real_step1_with_duration(self):
        m, rep = imp(HERE / "fixtures" / "real_cf59749" / "step1")
        self.assertEqual(rep["pstress_revision"], "cf59749")
        self.assertEqual(rep["duration_unit"], "us")
        self.assertEqual(len(m["sessions"]), 3)
        self.assertEqual(rep["unrecognized"], [])
        self.assertEqual(rep["unframed"], [])
        # every logged record line maps to exactly one statement, verbatim
        for s in m["sessions"]:
            src = HERE / "fixtures" / "real_cf59749" / "step1" / s["statements"][0]["source"]["file"]
            lines = src.read_text().split("\n")
            for st in s["statements"]:
                line = lines[st["source"]["line_start"] - 1]
                self.assertIn(st["sql"], line)
        st = m["sessions"][0]["statements"][0]
        self.assertEqual(st["sql"], "USE test")
        self.assertEqual(st["orig"]["timing"]["actual_unit"], "us")

    def test_real_step2_without_duration(self):
        d = HERE / "fixtures" / "real_cf59749" / "step2"
        with self.assertRaises(ReducerError):
            imp(d)  # step 2 needs the state left by step 1
        m, rep = imp(d, allow_empty_setup=True)
        total = sum(len(s["statements"]) for s in m["sessions"])
        self.assertEqual(total, rep["summary"]["statements"])
        self.assertGreater(rep["summary"]["by_status"].get("F", 0), 0)
        self.assertIsNone(m["sessions"][0]["statements"][0]["orig"]["timing"])


class PlainAndManifestTests(unittest.TestCase):
    def test_plain_sql(self):
        d = Path(tempfile.mkdtemp())
        try:
            p = write(d, "a.sql", "-- comment\nSELECT 1;\n\nSELECT ';';\n")
            m, rep = import_plain_sql([p], setup_spec="none", database="test", create_databases=None,
                                      loader="mysql-cli", snapshot_checksum="content", external_files=[],
                                      autocommit="server-default")
            self.assertEqual([s["sql"] for s in m["sessions"][0]["statements"]], ["SELECT 1", "SELECT ';'"])
            self.assertFalse(m["capture"]["complete"])
        finally:
            shutil.rmtree(d)

    def test_candidate_and_cost(self):
        m = {"format": "pstress-reducer-workload", "format_version": 1, "setup": {"kind": "none"},
             "sessions": [{"id": "a", "statements": [{"id": "a1", "sql": "S1"}, {"id": "a2", "sql": "S22", "pinned": True}]},
                          {"id": "b", "statements": [{"id": "b1", "sql": "S333"}]}],
             "groups": [["a1", "b1"]]}
        w = Workload(m)
        full = w.full_candidate()
        self.assertEqual(w.cost(full), (2, 2, 9))
        self.assertEqual(w.expand_removal(["a1"]), {"a1", "b1"})
        c = full.without({"b1"})
        self.assertEqual(w.cost(c), (1, 1, 5))
        self.assertEqual(Candidate.from_json(c.to_json()).hash, c.hash)
        sub = w.subset_manifest(c)
        self.assertEqual([s["id"] for s in sub["sessions"]], ["a"])
        self.assertEqual(sub["groups"], [])
        with self.assertRaises(ReducerError):
            Candidate({"a": ("a2", "a1")}).validate_against(w)
        bad = json.loads(json.dumps(m))
        bad["sessions"][1]["statements"][0]["id"] = "a1"
        with self.assertRaises(ReducerError):
            validate_manifest(bad)


if __name__ == "__main__":
    unittest.main()
