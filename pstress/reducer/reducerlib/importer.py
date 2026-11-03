"""Import pstress thread logs (or plain SQL / a user manifest) into a normalized manifest.

Record framing is derived from execute_sql() in src/random_test.cpp (verified at pstress
revision cf59749):

  [<YYYY-MM-DDTHH:MM:SS> <start>=><duration>ms ]* S <sql> rows:<n>\\n
  [<YYYY-MM-DDTHH:MM:SS> <start>=><duration>ms ]* F <sql>\\n
  Error <mysql_error text>\\n
  server gone, while processing <sql>\\n          (only on CR_SERVER_GONE/CR_SERVER_LOST)

* The optional timing prefix is written only with --log-query-duration.  At cf59749 both
  numbers are microseconds although the suffix says "ms".  When a statement's result is
  not logged (e.g. --log-failed-queries only) the prefix is still written, so prefixes can
  pile up in front of the next record; the last prefix belongs to the record.
* SQL is written verbatim and may contain newlines; an S record ends at the first line
  that ends in " rows:<n>", an F record at the first line starting with "Error ".
* Lines are written after mysql_real_query() returns, so an in-flight statement at crash
  time is only visible through the "server gone" diagnostic, if at all.

The importer never removes " S ", " F " or "rows:" from inside SQL text; it only parses
anchored prefixes and one trailing " rows:<n>" field.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .manifest import FORMAT, FORMAT_VERSION, Workload, validate_manifest
from .util import (TOOL_NAME, TOOL_VERSION, ReducerError, now_iso, read_json, sha256_file,
                   sha256_tree)

THREAD_FILE_RE = re.compile(r"^(?P<node>.+)_step_(?P<step>\d+)_thread-(?P<thread>\d+)\.sql$")
DDL_LOG_RE = re.compile(r"^(?P<node>.+)_ddl_step_(?P<step>\d+)\.log$")
_TS = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2} (-?\d+)=>(-?\d+)ms "
PREFIX_RE = re.compile(_TS)
LEADING_PREFIXES_RE = re.compile(r"^((?:" + r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2} -?\d+=>-?\d+ms " + r")*)")
PREFIX_CAPTURE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}) (-?\d+)=>(-?\d+)ms ")
ROWS_SUFFIX_RE = re.compile(r"^(?P<sql>.*) rows:(?P<rows>-?\d+)$", re.DOTALL)
ROWS_TAIL_RE = re.compile(r" rows:-?\d+$")
SERVER_GONE = "server gone, while processing "
REVISION_RE = re.compile(r"PStress v\S*?-(?P<rev>[0-9a-f]{7,40})\b")

# Known non-SQL lines written to thread logs (diagnostics, never executed).
KNOWN_DIAGNOSTICS = [re.compile(p) for p in (
    r"^waiting for metadata load to finish$",
    r"^Initial seed -?\d+$",
    r"^CURRENT SEED IS -?\d+$",
    r"^ value of rand_int\(\d+\) -?\d+$",
    r"^[A-Za-z0-9_\-]+=>.*$",                     # option dump "name=>value"
    r"^.*, total=>\d+, success=> ?\d+, skipped=> ?\d+$",  # per-action statistics
    r"^no index to drop .*$",
    r"^Error \d+: .*$",                           # connection-level errors
    r"^ ?initial setup failed.*$",
    r"^some other thread failed.*$",
    r"^Waiting for all thre?ads? .*$",
    r"^Failed to .*$",
    r"^Bulk insert failed .*$",
    r"^No vector tables:.*$",
    r"^mysql_(fetch_row|num_fields) called with nullptr arg!.*$",
    r"^Expected at least \d+ fields .*$",
    r"^Query execution .*$",
    r"^Vector support .*$",
)]

# pstress revisions whose duration prefix is known to be microseconds labelled "ms".
DURATION_UNIT_BY_REVISION = {"cf59749": "us"}


@dataclass
class FileImport:
    statements: list[dict] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)
    unframed: list[dict] = field(default_factory=list)
    incomplete: list[dict] = field(default_factory=list)
    unrecognized: list[dict] = field(default_factory=list)
    diagnostics: dict[str, int] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=lambda: {"S": 0, "F": 0, "LOST": 0,
                                                            "server_gone_dedup": 0,
                                                            "orphan_timing_prefixes": 0})


def _decode_lines(path: Path) -> tuple[list[str], bool]:
    data = path.read_bytes()
    ends_with_newline = data.endswith(b"\n") or not data
    text = data.decode("utf-8", "surrogateescape")
    lines = text.split("\n")
    if ends_with_newline and lines and lines[-1] == "":
        lines.pop()
    return lines, ends_with_newline


def _split_prefixes(line: str) -> tuple[list[tuple[str, int, int]], str]:
    m = LEADING_PREFIXES_RE.match(line)
    pre = m.group(1) if m else ""
    prefixes = [(a, int(b), int(c)) for a, b, c in PREFIX_CAPTURE_RE.findall(pre)]
    return prefixes, line[len(pre):]


def _diag_kind(text: str) -> str | None:
    for rx in KNOWN_DIAGNOSTICS:
        if rx.match(text):
            return rx.pattern
    return None


def parse_thread_log(path: Path, *, duration_unit: str = "unknown",
                     rel_name: str | None = None) -> FileImport:
    """Parse one pstress thread log into ordered statement records (no IDs yet)."""
    res = FileImport()
    lines, ends_nl = _decode_lines(path)
    fname = rel_name or path.name
    n = len(lines)

    def timing(prefixes):
        if not prefixes:
            return None
        if len(prefixes) > 1:
            res.counts["orphan_timing_prefixes"] += len(prefixes) - 1
        ts, start, dur = prefixes[-1]
        return {"wallclock": ts, "start_raw": start, "duration_raw": dur,
                "logged_unit": "ms", "actual_unit": duration_unit}

    def is_record_start(line: str) -> bool:
        _p, rest = _split_prefixes(line)
        return rest.startswith(" S ") or rest.startswith(" F ")

    i = 0
    while i < n:
        line = lines[i]
        prefixes, rest = _split_prefixes(line)
        lineno = i + 1
        if rest.startswith(" S "):
            body = rest[3:]
            buf = [body]
            j = i
            framed = False
            while True:
                if ROWS_TAIL_RE.search(buf[-1]):
                    framed = True
                    break
                if j + 1 >= n:
                    break
                nxt = lines[j + 1]
                if is_record_start(nxt):
                    break
                j += 1
                buf.append(nxt)
            if framed:
                m = ROWS_SUFFIX_RE.match("\n".join(buf))
                res.statements.append({
                    "sql": m.group("sql"),
                    "source": {"file": fname, "line_start": lineno, "line_end": j + 1},
                    "orig": {"status": "S", "rows": int(m.group("rows")), "timing": timing(prefixes)},
                })
                res.counts["S"] += 1
                if j > i:
                    res.warnings.append({"file": fname, "line": lineno,
                                         "message": f"multi-line S record ({j - i + 1} lines)"})
                i = j + 1
                continue
            item = {"file": fname, "line_start": lineno, "line_end": j + 1,
                    "text": "\n".join(buf)[:2000]}
            if j + 1 >= n:
                item["reason"] = "S record without ' rows:<n>' terminator at end of file"
                res.incomplete.append(item)
            else:
                item["reason"] = "S record without ' rows:<n>' terminator before next record"
                res.unframed.append(item)
            i = j + 1
            continue

        if rest.startswith(" F "):
            buf = [rest[3:]]
            j = i
            err_line = None
            while j + 1 < n:
                nxt = lines[j + 1]
                if nxt.startswith("Error ") and not re.match(r"^Error \d+: ", nxt):
                    err_line = j + 1
                    break
                if is_record_start(nxt):
                    break
                j += 1
                buf.append(nxt)
            if err_line is None:
                item = {"file": fname, "line_start": lineno, "line_end": j + 1, "text": "\n".join(buf)[:2000]}
                if j + 1 >= n:
                    item["reason"] = "F record without 'Error' line at end of file"
                    res.incomplete.append(item)
                else:
                    item["reason"] = "F record without 'Error' line before next record"
                    res.unframed.append(item)
                i = j + 1
                continue
            err = [lines[err_line][len("Error "):]]
            k = err_line
            # Error text may span lines (e.g. syntax errors quote multi-line SQL).
            while k + 1 < n:
                nxt = lines[k + 1]
                _p, nrest = _split_prefixes(nxt)
                if (is_record_start(nxt) or nrest.startswith(SERVER_GONE) or _diag_kind(nrest)):
                    break
                k += 1
                err.append(nxt)
            if len(err) > 1:
                res.warnings.append({"file": fname, "line": err_line + 1,
                                     "message": f"multi-line error message ({len(err)} lines) attached to F record"})
            stmt = {
                "sql": "\n".join(buf),
                "source": {"file": fname, "line_start": lineno, "line_end": k + 1},
                "orig": {"status": "F", "error": "\n".join(err), "server_gone": False,
                         "timing": timing(prefixes)},
            }
            res.statements.append(stmt)
            res.counts["F"] += 1
            if j > i:
                res.warnings.append({"file": fname, "line": lineno,
                                     "message": f"multi-line F record ({j - i + 1} SQL lines)"})
            i = k + 1
            continue

        if rest.startswith(SERVER_GONE):
            gone_sql_first = rest[len(SERVER_GONE):]
            prev = res.statements[-1] if res.statements else None
            if prev is not None and prev["orig"]["status"] == "F" and not prev["orig"].get("server_gone"):
                prev_lines = prev["sql"].split("\n")
                cand = [gone_sql_first] + lines[i + 1:i + len(prev_lines)]
                if cand == prev_lines:
                    prev["orig"]["server_gone"] = True
                    prev["source"]["line_end"] = i + len(prev_lines)
                    res.counts["server_gone_dedup"] += 1
                    if prefixes:
                        res.counts["orphan_timing_prefixes"] += len(prefixes)
                    i += len(prev_lines)
                    continue
            # No matching F record (e.g. failed queries were not logged): this diagnostic is
            # the only trace of the in-flight statement.  Import it, clearly labelled.
            res.statements.append({
                "sql": gone_sql_first,
                "source": {"file": fname, "line_start": lineno, "line_end": lineno},
                "orig": {"status": "LOST", "server_gone": True, "from_diagnostic": True,
                         "timing": timing(prefixes)},
            })
            res.counts["LOST"] += 1
            res.warnings.append({"file": fname, "line": lineno,
                                 "message": "statement imported from a 'server gone' diagnostic without a "
                                            "matching F record; multi-line SQL cannot be recovered from it"})
            i += 1
            continue

        if prefixes:
            res.counts["orphan_timing_prefixes"] += len(prefixes)
        if rest == "" and not prefixes:
            res.diagnostics["<empty line>"] = res.diagnostics.get("<empty line>", 0) + 1
            i += 1
            continue
        kind = _diag_kind(rest)
        if not kind and re.match(r"^\s{2,}\S", rest):
            kind = "<indented diagnostic text (e.g. multi-line option help)>"
        if kind:
            res.diagnostics[kind] = res.diagnostics.get(kind, 0) + 1
        elif rest:
            res.unrecognized.append({"file": fname, "line": lineno, "text": rest[:500]})
        i += 1

    if not ends_nl and n:
        res.warnings.append({"file": fname, "line": n, "message": "file does not end with a newline "
                             "(log may have been truncated while writing)"})
    return res


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
@dataclass
class LogSet:
    directory: Path
    node: str
    step: int
    files: dict[int, Path]  # thread -> path


def discover(incident_dir: Path, max_depth: int = 4) -> list[LogSet]:
    groups: dict[tuple[str, str, int], dict[int, Path]] = {}
    base_depth = len(incident_dir.resolve().parts)
    for dp, dn, fns in os.walk(incident_dir):
        depth = len(Path(dp).resolve().parts) - base_depth
        if depth >= max_depth:
            dn[:] = []
        for fn in fns:
            m = THREAD_FILE_RE.match(fn)
            if not m:
                continue
            key = (dp, m.group("node"), int(m.group("step")))
            groups.setdefault(key, {})[int(m.group("thread"))] = Path(dp) / fn
    return [LogSet(Path(d), node, step, dict(sorted(files.items())))
            for (d, node, step), files in sorted(groups.items(), key=lambda kv: (kv[0][2], kv[0][1], kv[0][0]))]


def select_logset(sets: list[LogSet], step: int | None, node: str | None) -> tuple[LogSet, list[str]]:
    notes: list[str] = []
    if not sets:
        raise ReducerError("no pstress thread logs (<node>_step_<N>_thread-<K>.sql) found")
    steps = sorted({s.step for s in sets})
    if step is None:
        if len(steps) > 1:
            raise ReducerError(f"incident contains several steps {steps}; select one with --step "
                               "(successive steps are never combined)")
        step = steps[0]
    cand = [s for s in sets if s.step == step]
    if not cand:
        raise ReducerError(f"no thread logs for step {step}; available steps: {steps}")
    nodes = sorted({s.node for s in cand})
    if len(nodes) > 1:
        if node is None:
            raise ReducerError(f"step {step} has logs for several nodes {nodes}: multi-node (PXC/group "
                               "replication) incidents are not supported by this version. Use --node to "
                               "import a single node's sessions only if you accept that this is not a "
                               "faithful multi-node reproduction.")
        notes.append(f"only node {node!r} imported; sessions of nodes {[x for x in nodes if x != node]} "
                     "were excluded (multi-node replay is not supported)")
    if node is not None:
        cand = [s for s in cand if s.node == node]
        if not cand:
            raise ReducerError(f"no thread logs for node {node!r} in step {step}; nodes: {nodes}")
    if len(cand) > 1:
        dirs = [str(s.directory) for s in cand]
        raise ReducerError(f"step {step} logs for node {cand[0].node!r} exist in several directories {dirs}; "
                           "point --incident-dir at the exact directory")
    return cand[0], notes


def detect_revision(directory: Path, node: str, step: int) -> str | None:
    for name in (f"{node}_ddl_step_{step}.log", f"{node}_step_{step}.log"):
        p = directory / name
        if p.is_file():
            try:
                with open(p, "r", encoding="utf-8", errors="replace") as f:
                    for _ in range(20):
                        line = f.readline()
                        if not line:
                            break
                        m = REVISION_RE.search(line)
                        if m:
                            return m.group("rev")
            except OSError:
                pass
    return None


# ---------------------------------------------------------------------------
# Setup description
# ---------------------------------------------------------------------------
def build_setup(spec: str, *, create_databases: list[str], loader: str,
                snapshot_checksum: str, external_files: list[str]) -> dict:
    if spec == "none":
        return {"kind": "none", "create_databases": create_databases,
                "notes": "server starts from a freshly initialized datadir"}
    if spec.startswith("sql:"):
        p = Path(spec[4:]).resolve()
        if not p.is_file():
            raise ReducerError(f"setup SQL file not found: {p}")
        return {"kind": "sql", "path": str(p), "sha256": sha256_file(p), "loader": loader,
                "create_databases": create_databases}
    if spec.startswith("snapshot:"):
        p = Path(spec[9:]).resolve()
        if not p.is_dir():
            raise ReducerError(f"setup snapshot directory not found: {p}")
        if not ((p / "mysql.ibd").exists() or (p / "ibdata1").exists()):
            raise ReducerError(f"{p} does not look like a MySQL datadir (no mysql.ibd/ibdata1)")
        ext = []
        for e in external_files:
            ep = Path(e).resolve()
            if not ep.exists():
                raise ReducerError(f"external file not found: {ep}")
            ext.append({"path": str(ep), "sha256": sha256_file(ep) if ep.is_file() else sha256_tree(ep)})
        keyring = []
        for cnf in sorted(p.glob("component_keyring_*.cnf")):
            try:
                kp = json.loads(cnf.read_text()).get("path")
            except (OSError, ValueError, AttributeError):
                kp = None
            if isinstance(kp, str) and kp:
                inside = (p / Path(kp).name).exists()
                keyring.append({"config": cnf.name, "path": kp, "file_in_snapshot": inside})
        return {"kind": "snapshot", "path": str(p), "checksum_mode": snapshot_checksum, "keyring": keyring,
                "sha256": sha256_tree(p, snapshot_checksum), "create_databases": create_databases,
                "external_files": ext,
                "notes": "must be a consistent offline copy (clean shutdown or prepared backup); it is "
                         "copied (never hard-linked) for every attempt and is never modified"}
    raise ReducerError("--setup must be 'none', 'sql:<file>' or 'snapshot:<datadir>'")


# ---------------------------------------------------------------------------
# Top-level import entry points
# ---------------------------------------------------------------------------
def _new_manifest(source: dict, setup: dict, sessions: list[dict], capture: dict) -> dict:
    return {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "created_by": f"{TOOL_NAME} {TOOL_VERSION}",
        "created_at": now_iso(),
        "source": source,
        "capture": capture,
        "setup": setup,
        "session_defaults": {"autocommit": "server-default", "charset": "utf8mb4"},
        "eof_policy": "hold-until-all-finished",
        "sessions": sessions,
        "groups": [],
    }


def import_pstress_logs(incident_dir: Path, *, step: int | None, node: str | None, setup_spec: str,
                        database: str, create_databases: list[str] | None, loader: str,
                        snapshot_checksum: str, external_files: list[str], include_failed: bool,
                        include_incomplete: bool, skip_unframed: bool, allow_empty_setup: bool,
                        pstress_revision: str | None, autocommit: Any,
                        duration_unit: str | None = None) -> tuple[dict, dict]:
    sets = discover(incident_dir)
    ls, notes = select_logset(sets, step, node)
    revision = pstress_revision or detect_revision(ls.directory, ls.node, ls.step)
    dunit = duration_unit or DURATION_UNIT_BY_REVISION.get((revision or "")[:7], "unknown")

    report: dict[str, Any] = {
        "selected": {"directory": str(ls.directory), "node": ls.node, "step": ls.step,
                     "threads": sorted(ls.files)},
        "available_steps": sorted({s.step for s in sets}),
        "pstress_revision": revision,
        "duration_unit": dunit,
        "notes": list(notes),
        "files": [], "warnings": [], "unframed": [], "incomplete": [], "unrecognized": [],
        "excluded_statements": [],
    }
    limitations = [
        "statements are logged after mysql_real_query() returns: the relative order of statements in "
        "different sessions is not captured, and a statement in flight at crash time is present only "
        "via a 'server gone' diagnostic, if at all",
        "a random seed does not make server-side interleavings deterministic",
    ]
    inherent = len(limitations)
    if revision is None:
        limitations.append("pstress revision unknown: timing prefixes are kept raw and their unit is unknown")
    elif dunit == "unknown":
        notes.append(f"duration unit of pstress revision {revision} is not known to the importer; timing is kept "
                     "raw (pass --duration-unit us if this build logs microseconds like cf59749)")

    if ls.step > 1 and setup_spec == "none" and not allow_empty_setup:
        prev = ls.directory.parent / str(ls.step - 1)
        hint = ""
        for cand in (prev / "data", ls.directory / "data"):
            if cand.is_dir():
                hint = f" A saved datadir exists at {cand}; consider --setup snapshot:{cand}."
                break
        raise ReducerError(
            f"step {ls.step} runs on the state left by step {ls.step - 1}; an empty database is not "
            f"equivalent. Provide that state with --setup sql:<dump> or --setup snapshot:<datadir>, or pass "
            f"--allow-empty-setup to accept the difference explicitly.{hint}")

    sessions = []
    any_success_logged = False
    for thread, path in ls.files.items():
        fi = parse_thread_log(path, duration_unit=dunit, rel_name=path.name)
        report["files"].append({"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size,
                                "thread": thread, "counts": fi.counts, "diagnostics": fi.diagnostics})
        report["warnings"] += fi.warnings
        report["unframed"] += fi.unframed
        report["incomplete"] += fi.incomplete
        report["unrecognized"] += fi.unrecognized
        if fi.counts["S"]:
            any_success_logged = True
        stmts = []
        seq = 0
        for rec in fi.statements:
            if rec["orig"]["status"] == "F" and not include_failed:
                report["excluded_statements"].append({"file": path.name, "line": rec["source"]["line_start"],
                                                      "reason": "failed statement excluded by --exclude-failed"})
                continue
            seq += 1
            rec = dict(rec)
            rec["id"] = f"t{thread}.{seq:06d}"
            stmts.append(rec)
        if include_incomplete:
            for inc in fi.incomplete:
                body = inc["text"]
                m = ROWS_SUFFIX_RE.match(body)
                seq += 1
                stmts.append({"id": f"t{thread}.{seq:06d}", "sql": m.group("sql") if m else body,
                              "source": {"file": path.name, "line_start": inc["line_start"],
                                         "line_end": inc["line_end"]},
                              "orig": {"status": "INCOMPLETE"}})
        if skip_unframed:
            for u in fi.unframed:
                report["excluded_statements"].append({"file": path.name, "line": u["line_start"],
                                                      "reason": "unframed record skipped by --skip-unframed"})
        db = database
        for st in stmts:
            m = re.match(r"^\s*USE\s+`?([^`\s;]+)`?\s*;?\s*$", st["sql"], re.IGNORECASE)
            if m:
                db = m.group(1)
            break
        session = {"id": f"{ls.node}:step{ls.step}:thread-{thread}", "node": ls.node, "step": ls.step,
                   "thread": thread, "database": db, "autocommit": autocommit, "charset": "utf8mb4",
                   "pinned": False, "statements": stmts}
        if not stmts:
            report["notes"].append(f"thread {thread} has no statements; session omitted")
            continue
        sessions.append(session)

    if report["unframed"] and not skip_unframed:
        report["status"] = "needs-correction"
        raise_import = ReducerError(
            f"{len(report['unframed'])} record(s) cannot be framed unambiguously (first: "
            f"{report['unframed'][0]['file']}:{report['unframed'][0]['line_start']}). Fix the log, supply a "
            f"normalized manifest, or pass --skip-unframed to exclude them explicitly.", 7)
        raise_import.report = report  # type: ignore[attr-defined]
        raise raise_import

    if not any_success_logged:
        limitations.append("no successful (S) records were logged (run without --log-all-queries/"
                           "--log-succeeded-queries?): the captured workload is incomplete")
    if report["incomplete"] and not include_incomplete:
        limitations.append(f"{len(report['incomplete'])} incomplete final record(s) were not imported "
                           "(see import report; --include-incomplete imports them)")
    if not include_failed:
        limitations.append("failed (F) statements were excluded by request; they may matter for locking "
                           "and transaction state")
    if report["unframed"]:
        limitations.append(f"{len(report['unframed'])} unframed record(s) skipped by request")

    dbs = create_databases if create_databases is not None else sorted({s["database"] for s in sessions if s["database"]})
    setup = build_setup(setup_spec, create_databases=dbs, loader=loader,
                        snapshot_checksum=snapshot_checksum, external_files=external_files)
    if ls.step > 1 and setup_spec == "none":
        limitations.append(f"step {ls.step} imported with an EMPTY setup (--allow-empty-setup): the state "
                           f"left by step {ls.step - 1} is missing")
    source = {"kind": "pstress-thread-logs", "incident_dir": str(incident_dir.resolve()),
              "directory": str(ls.directory.resolve()), "node": ls.node, "step": ls.step,
              "pstress_revision": revision, "duration_unit": dunit,
              "files": [{"path": f["path"], "sha256": f["sha256"], "bytes": f["bytes"]} for f in report["files"]]}
    gaps = [x for x in limitations[inherent:] if not x.startswith("pstress revision unknown")]
    capture = {"complete": not gaps and any_success_logged, "limitations": limitations}
    manifest = _new_manifest(source, setup, sessions, capture)
    validate_manifest(manifest)
    report["status"] = "ok"
    report["summary"] = summarize(manifest)
    return manifest, report


def import_plain_sql(files: list[Path], *, setup_spec: str, database: str, create_databases: list[str] | None,
                     loader: str, snapshot_checksum: str, external_files: list[str], autocommit: Any) -> tuple[dict, dict]:
    """Plain SQL input: one file per session, one statement per line (pstress converter format)."""
    sessions = []
    report: dict[str, Any] = {"files": [], "warnings": [], "notes": [], "unframed": [], "incomplete": [],
                              "unrecognized": [], "excluded_statements": []}
    for idx, path in enumerate(files):
        lines, _ = _decode_lines(path)
        stmts = []
        skipped_comments = 0
        for ln, line in enumerate(lines, 1):
            s = line.strip()
            if not s:
                continue
            if s.startswith("--") or s.startswith("#"):
                skipped_comments += 1
                continue
            if s.endswith(";"):
                s = s[:-1].rstrip()
            stmts.append({"id": f"p{idx}.{len(stmts) + 1:06d}", "sql": s,
                          "source": {"file": path.name, "line_start": ln, "line_end": ln},
                          "orig": {"status": "UNKNOWN"}})
        if skipped_comments:
            report["notes"].append(f"{path.name}: {skipped_comments} comment line(s) not imported")
        report["files"].append({"path": str(path.resolve()), "sha256": sha256_file(path),
                                "bytes": path.stat().st_size, "statements": len(stmts)})
        sessions.append({"id": f"plain:{idx}:{path.name}", "database": database, "autocommit": autocommit,
                         "charset": "utf8mb4", "pinned": False, "statements": stmts})
    dbs = create_databases if create_databases is not None else [database]
    setup = build_setup(setup_spec, create_databases=dbs, loader=loader,
                        snapshot_checksum=snapshot_checksum, external_files=external_files)
    capture = {"complete": False, "limitations": [
        "plain SQL input: no success/failure, row-count or timing metadata",
        "plain SQL framing is one statement per line; multi-line statements are not supported in this format",
    ]}
    source = {"kind": "plain-sql", "files": report["files"]}
    manifest = _new_manifest(source, setup, sessions, capture)
    validate_manifest(manifest)
    report["status"] = "ok"
    report["summary"] = summarize(manifest)
    return manifest, report


def import_manifest(path: Path) -> tuple[dict, dict]:
    m = read_json(path)
    validate_manifest(m)
    w = Workload(m)
    report = {"status": "ok", "notes": [f"validated user-supplied manifest {path}"], "files": [],
              "warnings": [], "unframed": [], "incomplete": [], "unrecognized": [], "excluded_statements": [],
              "summary": summarize(m)}
    setup = m["setup"]
    if setup["kind"] == "sql" and Path(setup["path"]).is_file():
        actual = sha256_file(setup["path"])
        if setup.get("sha256") and setup["sha256"] != actual:
            raise ReducerError(f"setup file checksum mismatch for {setup['path']}")
        setup["sha256"] = actual
    _ = w
    return m, report


def summarize(m: dict) -> dict:
    sessions = m["sessions"]
    total = sum(len(s["statements"]) for s in sessions)
    by_status: dict[str, int] = {}
    for s in sessions:
        for st in s["statements"]:
            k = st.get("orig", {}).get("status", "UNKNOWN")
            by_status[k] = by_status.get(k, 0) + 1
    return {"sessions": len(sessions), "statements": total, "by_status": by_status,
            "per_session": {s["id"]: len(s["statements"]) for s in sessions},
            "sql_bytes": sum(len(st["sql"].encode("utf-8", "surrogateescape")) for s in sessions for st in s["statements"])}


def render_report_text(report: dict, manifest: dict | None) -> str:
    out = []
    sel = report.get("selected")
    if sel:
        out.append(f"Selected: node={sel['node']} step={sel['step']} dir={sel['directory']} threads={sel['threads']}")
        out.append(f"pstress revision: {report.get('pstress_revision')}  duration unit: {report.get('duration_unit')}")
    s = report.get("summary")
    if s:
        out.append(f"Sessions: {s['sessions']}  statements: {s['statements']}  by status: {s['by_status']}  "
                   f"SQL bytes: {s['sql_bytes']}")
    for f in report.get("files", []):
        out.append(f"  {f['path']}  sha256={f['sha256'][:16]}…  counts={f.get('counts', f.get('statements'))}")
    for key in ("notes",):
        for n in report.get(key, []):
            out.append(f"NOTE: {n}")
    for w in report.get("warnings", [])[:50]:
        out.append(f"WARN: {w['file']}:{w['line']}: {w['message']}")
    if len(report.get("warnings", [])) > 50:
        out.append(f"WARN: … {len(report['warnings']) - 50} more warnings in import-report.json")
    for u in report.get("unframed", []):
        out.append(f"UNFRAMED: {u['file']}:{u['line_start']}-{u['line_end']}: {u['reason']}")
    for u in report.get("incomplete", []):
        out.append(f"INCOMPLETE: {u['file']}:{u['line_start']}-{u['line_end']}: {u['reason']}")
    for u in report.get("unrecognized", [])[:50]:
        out.append(f"UNRECOGNIZED: {u['file']}:{u['line']}: {u['text'][:120]}")
    if manifest:
        for lim in manifest.get("capture", {}).get("limitations", []):
            out.append(f"LIMITATION: {lim}")
        out.append(f"Setup: {manifest['setup']['kind']} {manifest['setup'].get('path', '')}")
        for k in manifest["setup"].get("keyring") or []:
            where = ("inside the snapshot: each attempt's copy is repointed to its own file"
                     if k["file_in_snapshot"] else "OUTSIDE the snapshot: it must stay readable at that path")
            out.append(f"Keyring: {k['config']} -> {k['path']} ({where})")
    return "\n".join(out) + "\n"
