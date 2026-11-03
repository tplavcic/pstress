"""Milestone B (+C end-to-end): isolated replay against real managed mysqld instances.

Requires a MySQL/Percona Server mysqld binary: PRD_MYSQLD=/path/to/mysqld (default: mysqld
on PATH) and PyMySQL.  Tests are skipped when no server binary is available.  The slow
end-to-end reduction tests run only with PRD_SLOW=1.
"""

import json
import os
import shutil
import signal

import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

from reducerlib.config import DEFAULTS, _merge, finalize  # noqa: E402
from reducerlib.lifecycle import REGISTRY, ensure_template  # noqa: E402
from reducerlib.manifest import Workload  # noqa: E402
from reducerlib.trial import make_context, run_attempt  # noqa: E402
from reducerlib.replay import merged_timeline  # noqa: E402

MYSQLD = os.environ.get("PRD_MYSQLD") or shutil.which("mysqld") or ""
try:
    import importlib.util
    HAVE_PYMYSQL = importlib.util.find_spec("pymysql") is not None
except ImportError:
    HAVE_PYMYSQL = False
SKIP = not (MYSQLD and os.access(MYSQLD, os.X_OK) and HAVE_PYMYSQL)
SLOW = os.environ.get("PRD_SLOW") == "1"
WRAPPER = HERE / "helpers" / "crashing_mysqld.py"


def manifest(sessions, setup_sql=None, tmp=None):
    setup = {"kind": "none", "create_databases": ["test"]}
    if setup_sql:
        p = Path(tmp) / "setup.sql"
        p.write_text(setup_sql)
        setup = {"kind": "sql", "path": str(p), "loader": "mysql-cli", "create_databases": ["test"]}
    ss = []
    for i, stmts in enumerate(sessions):
        ss.append({"id": f"s{i}", "database": "test", "autocommit": "server-default",
                   "statements": [{"id": f"s{i}.{j:03d}", "sql": q} for j, q in enumerate(stmts)]})
    return {"format": "pstress-reducer-workload", "format_version": 1, "setup": setup, "sessions": ss,
            "groups": [], "eof_policy": "hold-until-all-finished"}


@unittest.skipIf(SKIP, "no mysqld binary (set PRD_MYSQLD) or PyMySQL not installed")
class ReplayIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shared = Path(tempfile.mkdtemp(prefix="prd-it-"))
        cfg = cls.base_cfg(None)
        cls.template = ensure_template(cfg, cls.shared)

    @classmethod
    def tearDownClass(cls):
        REGISTRY.kill_all()
        shutil.rmtree(cls.shared, ignore_errors=True)

    @classmethod
    def base_cfg(cls, overrides):
        raw = {"server": {"mysqld": MYSQLD, "startup_timeout": 90},
               "trial": {"replay_timeout": 60, "query_timeout": 60, "post_crash_wait": 15},
               "oracle": {"kind": "crash", "required": ["got signal 11"]},
               "retention": {"keep_target_trials": 50, "keep_other_trials": 50}}
        cfg = _merge(DEFAULTS, raw)
        if overrides:
            for sect, vals in overrides.items():
                if isinstance(vals, dict):
                    cfg[sect].update(vals)
                else:
                    cfg[sect] = vals
        if getattr(cls, "template", None):
            cfg["server"]["template_datadir"] = str(cls.template)
        finalize(cfg, Path.cwd())
        return cfg

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="prd-case-", dir=self.shared))

    def ctx(self, m, overrides=None):
        cfg = self.base_cfg(overrides)
        w = Workload(m)
        return make_context(w, cfg, "", self.tmp), w

    def attempt(self, m, overrides=None, purpose="test"):
        ctx, w = self.ctx(m, overrides)
        rec = run_attempt(ctx, w.full_candidate(), purpose)
        return rec, ctx

    # ------------------------------------------------------------------
    def test_lock_overlap_is_not_serialized(self):
        m = manifest([
            ["BEGIN", "UPDATE t SET v = 1 WHERE id = 1", "SELECT SLEEP(1)", "COMMIT"],
            ["SELECT SLEEP(0.3)", "UPDATE t SET v = v + 10 WHERE id = 1", "INSERT INTO t VALUES (2, 99)"],
        ], setup_sql="CREATE TABLE test.t (id INT PRIMARY KEY, v INT) ENGINE=InnoDB;\nINSERT INTO test.t VALUES (1, 0);\n",
            tmp=self.tmp)
        rec, ctx = self.attempt(m, {"oracle": {"kind": "sql_probe", "required": [],
                                               "probe": {"query": "SELECT v FROM test.t WHERE id = 1",
                                                         "expect_regex": "^11$"}}})
        self.assertEqual(rec["outcome"], "TARGET", rec)
        self.assertFalse(rec["replay"]["timed_out"])
        tl = merged_timeline(ctx.workdir / "trials" / f"{rec['attempt']:06d}")
        t_commit_send = next(e["t"] for e in tl if e.get("ev") == "send" and e.get("id") == "s0.003")
        t_b_update_send = next(e["t"] for e in tl if e.get("ev") == "send" and e.get("id") == "s1.001")
        t_b_update_done = next(e["t"] for e in tl if e.get("ev") == "done" and e.get("id") == "s1.001")
        self.assertLess(t_b_update_send, t_commit_send, "B must be blocked while A holds the lock")
        self.assertGreaterEqual(t_b_update_done, t_commit_send, "B finishes only after A commits")

    def test_session_state_persists_and_no_implicit_commit(self):
        m = manifest([
            ["CREATE TEMPORARY TABLE tmp (a INT)", "INSERT INTO tmp VALUES (5)",
             "PREPARE p FROM 'SELECT a FROM tmp WHERE a = ?'", "SET @x = 5", "EXECUTE p USING @x",
             "INSERT INTO t2 SELECT a * 2 FROM tmp", "SET autocommit = 0", "INSERT INTO t2 VALUES (7)"],
            ["SELECT 1"],
        ], setup_sql="CREATE TABLE test.t2 (a INT) ENGINE=InnoDB;\n", tmp=self.tmp)
        rec, _ = self.attempt(m, {"oracle": {"kind": "sql_probe", "required": [],
                                             "probe": {"query": "SELECT GROUP_CONCAT(a ORDER BY a) FROM test.t2",
                                                       "expect_regex": "^10$"}}})
        self.assertEqual(rec["outcome"], "TARGET", rec)
        self.assertEqual(rec["replay"]["sessions"]["s0"]["errors"], [])

    def test_setup_with_delimiter_and_multi_result_call(self):
        setup = ("CREATE TABLE test.t3 (a INT);\nDELIMITER //\nCREATE PROCEDURE test.p()\nBEGIN\n"
                 "  SELECT 1; SELECT 2; INSERT INTO test.t3 VALUES (3);\nEND//\nDELIMITER ;\n")
        m = manifest([["CALL p()", "CALL p()", "SELECT COUNT(*) FROM t3"]], setup_sql=setup, tmp=self.tmp)
        rec, _ = self.attempt(m, {"oracle": {"kind": "sql_probe", "required": [],
                                             "probe": {"query": "SELECT SUM(a) FROM test.t3", "expect_regex": "^6$"}}})
        self.assertEqual(rec["outcome"], "TARGET", rec)
        self.assertEqual(rec["replay"]["sessions"]["s0"]["errors"], [])

    def test_snapshot_setup_is_copied_and_never_modified(self):
        from reducerlib.importer import build_setup
        from reducerlib.lifecycle import ManagedServer
        from reducerlib.util import sha256_tree
        cfg = self.base_cfg(None)
        snapdir = self.tmp / "snapwork"
        snapdir.mkdir()
        srv = ManagedServer(cfg, snapdir, self.tmp, "8.0.0")
        srv.prepare(self.template)
        srv.start()
        ok, why = srv.wait_ready("", 90)
        self.assertTrue(ok, why)
        c = srv.connect("")
        c.cursor().execute("CREATE DATABASE test")
        c.cursor().execute("CREATE TABLE test.prev (a INT)")
        c.cursor().execute("INSERT INTO test.prev VALUES (41)")
        c.close()
        srv.stop("", "shutdown", 60)  # clean shutdown -> consistent offline snapshot
        snapshot = snapdir / "data"
        setup = build_setup(f"snapshot:{snapshot}", create_databases=["test"], loader="mysql-cli",
                            snapshot_checksum="content", external_files=[])
        m = manifest([["UPDATE prev SET a = a + 1"]])
        m["setup"] = setup
        before = sha256_tree(snapshot)
        rec, _ = self.attempt(m, {"oracle": {"kind": "sql_probe", "required": [],
                                             "probe": {"query": "SELECT a FROM test.prev", "expect_regex": "^42$"}}})
        self.assertEqual(rec["outcome"], "TARGET", rec)
        self.assertEqual(sha256_tree(snapshot), before)

    def test_server_errors_above_3000_continue_the_session(self):
        # 3141 (invalid JSON) is a server error, not a client/transport error
        m = manifest([["SELECT JSON_EXTRACT('not json', '$')", "CREATE TABLE after_err (a INT)"]])
        t0 = time.monotonic()
        rec, _ = self.attempt(m, {"oracle": {"kind": "sql_probe", "required": [],
                                             "probe": {"query": "SHOW TABLES FROM test", "expect_regex": "after_err"}}})
        self.assertEqual(rec["outcome"], "TARGET", rec)
        errs = rec["replay"]["sessions"]["s0"]["errors"]
        self.assertEqual([(e["errno"], e["class"]) for e in errs], [(3141, "server")])
        self.assertLess(time.monotonic() - t0, 20)

    def test_fresh_state_every_attempt(self):
        m = manifest([["CREATE TABLE fresh (a INT)", "INSERT INTO fresh VALUES (1)"]])
        ov = {"oracle": {"kind": "sql_probe", "required": [],
                         "probe": {"query": "SELECT COUNT(*) FROM test.fresh", "expect_regex": "^1$"}}}
        ctx, w = self.ctx(m, ov)
        r1 = run_attempt(ctx, w.full_candidate(), "a")
        r2 = run_attempt(ctx, w.full_candidate(), "b")
        self.assertEqual((r1["outcome"], r2["outcome"]), ("TARGET", "TARGET"))
        self.assertEqual(r2["replay"]["sessions"]["s0"]["errors"], [])
        self.assertFalse((ctx.workdir / "trials" / "000001" / "data").exists(), "datadir must be cleaned")

    def test_blocked_query_times_out_and_unrelated_processes_survive(self):
        fake_dir = self.tmp / "unrelated"
        fake_dir.mkdir()
        fake = fake_dir / "mysqld"
        fake.write_text("#!/bin/sh\nexec sleep 300\n")
        fake.chmod(0o755)
        unrelated = subprocess.Popen([str(fake)], start_new_session=True)
        try:
            m = manifest([
                ["BEGIN", "SELECT * FROM t WHERE id = 1 FOR UPDATE", "SELECT SLEEP(60)"],
                ["SELECT SLEEP(0.3)", "UPDATE t SET v = 5 WHERE id = 1"],
            ], setup_sql="CREATE TABLE test.t (id INT PRIMARY KEY, v INT) ENGINE=InnoDB;\n"
                         "INSERT INTO test.t VALUES (1, 0);\n", tmp=self.tmp)
            t0 = time.monotonic()
            rec, ctx = self.attempt(m, {"trial": {"replay_timeout": 3}})
            elapsed = time.monotonic() - t0
            self.assertEqual(rec["outcome"], "UNRESOLVED", rec)
            self.assertLess(elapsed, 40)
            self.assertIsNone(unrelated.poll(), "an unrelated process named mysqld was touched")
            pid_file = ctx.workdir / "trials" / f"{rec['attempt']:06d}" / "mysqld.pid"
            if pid_file.exists():
                pid = int(pid_file.read_text().strip())
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            self.assertTrue(rec["server_exit"]["expected"])
        finally:
            unrelated.kill()
            unrelated.wait()

    def test_hook_timeout_is_bounded_and_invalid(self):
        m = manifest([["SELECT 1"]])
        t0 = time.monotonic()
        rec, _ = self.attempt(m, {"hooks": {"prepare": ["sleep", "30"], "timeout": 2}})
        self.assertLess(time.monotonic() - t0, 15)
        self.assertEqual(rec["outcome"], "INVALID")
        self.assertTrue(rec["infra"])

    def test_startup_error_is_invalid_not_target(self):
        m = manifest([["SELECT 1"]])
        rec, _ = self.attempt(m, {"server": {"extra_args": ["--no-such-option-xyz"], "startup_timeout": 30},
                                  "oracle": {"kind": "crash", "required": ["no-such-option|ERROR"]}})
        self.assertEqual(rec["outcome"], "INVALID")
        self.assertTrue(rec["infra"])

    def test_reducer_kill_and_startup_text_never_count(self):
        # "ready for connections" is in the startup phase; the reducer then kills the server.
        m = manifest([["SELECT 1"]])
        rec, _ = self.attempt(m, {"oracle": {"kind": "crash", "required": ["ready for connections"]}})
        self.assertEqual(rec["outcome"], "NO_TARGET")
        self.assertTrue(rec["server_exit"]["expected"])
        self.assertEqual(rec["server_exit"]["phase"], "shutdown")

    def test_disconnect_without_crash_is_not_target(self):
        m = manifest([["SELECT 1", "KILL CONNECTION_ID()", "SELECT 2"], ["SELECT SLEEP(0.5)"]])
        rec, _ = self.attempt(m, {"oracle": {"kind": "crash", "required": ["."]}})
        self.assertEqual(rec["outcome"], "NO_TARGET")
        self.assertEqual(rec["replay"]["sessions"]["s0"]["state"], "aborted")

    def test_injected_crash_target_unrelated_crash_and_stale_logs(self):
        os.environ["PRD_REAL_MYSQLD"] = MYSQLD
        os.environ["PRD_CRASH_MARKERS"] = "CRASH_A,CRASH_B"
        crash = manifest([["SELECT 'CRASH_A'", "SELECT SLEEP(5)"], ["SELECT SLEEP(0.2)", "SELECT 'CRASH_B'"]])
        ov = {"server": {"mysqld": str(WRAPPER)}}
        rec, ctx = self.attempt(crash, ov)
        self.assertEqual(rec["outcome"], "TARGET", rec)
        self.assertFalse(rec["server_exit"]["expected"])
        # same crash, different required signature -> unrelated crash, recorded distinctly
        rec2, _ = self.attempt(crash, {"server": {"mysqld": str(WRAPPER)},
                                       "oracle": {"kind": "crash", "required": ["Assertion failure: never"]}})
        self.assertEqual(rec2["outcome"], "NO_TARGET")
        self.assertTrue(rec2["other_failure"])
        # a following attempt without the trigger must not see the previous crash report
        quiet = manifest([["SELECT 'CRASH_A'"], ["SELECT 1"]])
        rec3, _ = self.attempt(quiet, ov)
        self.assertEqual(rec3["outcome"], "NO_TARGET")


@unittest.skipIf(SKIP or not SLOW, "slow end-to-end tests: set PRD_SLOW=1 (and PRD_MYSQLD)")
class EndToEndReduction(unittest.TestCase):
    """Synthetic incident: crash needs one statement from each of two sessions."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="prd-e2e-"))
        self.env = dict(os.environ, PRD_REAL_MYSQLD=MYSQLD, PRD_CRASH_MARKERS="CRASH_A,CRASH_B")
        noise = lambda k: [f"SELECT {k * 10 + i}" for i in range(5)]  # noqa: E731
        # The marker statements keep running (SLEEP) so the injected crash happens while they execute.
        m = manifest([noise(1) + ["SELECT 'CRASH_A', SLEEP(3)"] + noise(2),
                      noise(3) + ["SELECT 'CRASH_B', SLEEP(1)"] + noise(4),
                      noise(5) + noise(6)])
        (self.tmp / "workload").mkdir()
        (self.tmp / "workload" / "manifest.json").write_text(json.dumps(m))
        cfg = {"server": {"mysqld": str(WRAPPER), "startup_timeout": 90},
               "trial": {"replay_timeout": 60, "post_crash_wait": 15},
               "oracle": {"kind": "crash", "required": ["got signal 11"]},
               "policy": {"baseline_attempts": 2, "candidate_max_valid": 1, "candidate_accept_hits": 1,
                          "final_attempts": 3, "final_required_hits": 2, "probe_empty_workload": True}}
        (self.tmp / "config.json").write_text(json.dumps(cfg))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *args, timeout=1800):
        return subprocess.run([sys.executable, str(ROOT / "pstress_reducer.py"), *args], env=self.env,
                              capture_output=True, text=True, timeout=timeout)

    def test_interrupt_resume_reduce_and_replay_bundle(self):
        wd = self.tmp / "work"
        p = subprocess.Popen([sys.executable, str(ROOT / "pstress_reducer.py"), "reduce", "--workload",
                              str(self.tmp / "workload"), "--config", str(self.tmp / "config.json"),
                              "--workdir", str(wd)], env=self.env, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        # wait until reduction passes are running, then interrupt
        deadline = time.time() + 600
        while time.time() < deadline and p.poll() is None:
            j = wd / "journal.jsonl"
            if j.exists() and j.read_text().count('"type": "decision"') >= 2:
                break
            time.sleep(1)
        p.send_signal(signal.SIGINT)
        out, _ = p.communicate(timeout=120)
        self.assertEqual(p.returncode, 130, out)
        state = json.loads((wd / "state.json").read_text())
        self.assertTrue((wd / "candidates" / f"{state['best']}.json").exists())
        leftovers = subprocess.run(["pgrep", "-f", str(wd)], capture_output=True, text=True).stdout.split()
        self.assertEqual(leftovers, [], "owned processes survived the interrupt")
        r = self.run_cli("resume", "--workdir", str(wd))
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        bundle = wd / "bundle"
        m = json.loads((bundle / "manifest.json").read_text())
        kept = {s["id"]: [st["sql"] for st in s["statements"]] for s in m["sessions"]}
        self.assertEqual(kept, {"s0": ["SELECT 'CRASH_A', SLEEP(3)"], "s1": ["SELECT 'CRASH_B', SLEEP(1)"]})
        # replay the exported bundle independently of the reduction work directory
        r = self.run_cli("replay", "--workload", str(bundle), "--config", str(bundle / "config.json"),
                         "--workdir", str(self.tmp / "replay"), "--attempts", "2")
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        self.assertIn('"target": 2', r.stdout)




if __name__ == "__main__":
    unittest.main()
