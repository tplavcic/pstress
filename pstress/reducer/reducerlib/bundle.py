"""Export a reduced candidate as a self-describing replay bundle, and load workloads
from a manifest file or a bundle directory (relative setup paths resolve against it)."""

from __future__ import annotations

import copy
import json
import shlex
import shutil
from pathlib import Path

from .manifest import Candidate, Workload
from .util import (TOOL_NAME, TOOL_VERSION, ReducerError, atomic_write_json, atomic_write_text, now_iso,
                   read_json, redact_argv, sha256_file)


def load_workload(path: str | Path) -> tuple[Workload, Path]:
    """Accepts a manifest JSON file, or a directory containing manifest.json (bundle/import dir)."""
    p = Path(path)
    if p.is_dir():
        p = p / "manifest.json"
    if not p.is_file():
        raise ReducerError(f"workload manifest not found: {p}")
    m = read_json(p)
    setup = m.get("setup") or {}
    if setup.get("path") and not Path(setup["path"]).is_absolute():
        setup["path"] = str((p.parent / setup["path"]).resolve())
    for e in setup.get("external_files") or []:
        if e.get("path") and not Path(e["path"]).is_absolute():
            e["path"] = str((p.parent / e["path"]).resolve())
    return Workload(m), p


def _session_filename(i: int, sid: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in sid)
    return f"{i:02d}_{safe}.sql"


def export_bundle(out_dir: Path, w: Workload, cand: Candidate, *, cfg: dict, env_fp: dict, history: list,
                  validation: dict | None, baseline: dict | None, evidence_dir: Path | None,
                  status: str, include_snapshot: bool = False, notes: list[str] | None = None) -> Path:
    out_dir = Path(out_dir)
    if out_dir.exists():
        shutil.rmtree(out_dir)
    (out_dir / "sessions").mkdir(parents=True)
    (out_dir / "setup").mkdir()
    m = w.subset_manifest(cand)
    setup = m["setup"]
    portable = True
    missing: list[str] = []
    if setup["kind"] == "sql":
        src = Path(setup["path"])
        shutil.copy2(src, out_dir / "setup" / "setup.sql")
        setup["path"] = "setup/setup.sql"
        setup["sha256"] = sha256_file(out_dir / "setup" / "setup.sql")
    elif setup["kind"] == "snapshot":
        if include_snapshot:
            shutil.copytree(setup["path"], out_dir / "setup" / "datadir", symlinks=True)
            setup["path"] = "setup/datadir"
        else:
            portable = False
            missing.append(f"setup snapshot datadir {setup['path']} (sha256 {setup.get('sha256')}, "
                           f"checksum mode {setup.get('checksum_mode')})")
        for e in setup.get("external_files") or []:
            portable = False
            missing.append(f"external file {e['path']} (sha256 {e.get('sha256')})")
    atomic_write_json(out_dir / "manifest.json", m)
    w2 = Workload(m)
    session_files = []
    for i, s in enumerate(w2.sessions):
        fn = _session_filename(i, s["id"])
        atomic_write_text(out_dir / "sessions" / fn, w2.session_sql_text(s["id"]))
        session_files.append(fn)
    bundle_cfg = copy.deepcopy(cfg)
    bundle_cfg["workdir"] = None
    atomic_write_json(out_dir / "config.json", bundle_cfg)
    srv = cfg["server"]
    atomic_write_json(out_dir / "environment.json", {
        "server_version": env_fp.get("server_version"), "mysqld": srv["mysqld"],
        "mysqld_sha256": env_fp.get("mysqld_sha256"), "basedir": srv["basedir"],
        "startup_args_user": redact_argv(srv["extra_args"]), "no_defaults": srv["no_defaults"],
        "managed_args": srv["managed_args"], "initialize_args": srv["initialize_args"],
        "template_datadir": srv["template_datadir"], "user": srv["user"],
        "credentials": f"password from environment variable {srv['password_env']!r} or credentials_file; "
                       "never stored in the bundle",
    })
    atomic_write_json(out_dir / "replay-policy.json", {
        "scheduling": "free-run: one connection per session, common start gate, no per-statement gating",
        "eof_policy": m.get("eof_policy"), "trial": cfg["trial"],
        "note": "replay timing is not deterministic; hit rates below are measured, not guaranteed"})
    atomic_write_json(out_dir / "oracle.json", cfg["oracle"])
    atomic_write_json(out_dir / "history.json", history)
    atomic_write_json(out_dir / "validation.json", {"status": status, "baseline": baseline,
                                                    "final_validation": validation})
    if evidence_dir and evidence_dir.is_dir():
        shutil.copytree(evidence_dir, out_dir / "evidence")
    cost = w.cost(cand)
    lines = [
        "# pstress reducer bundle",
        "",
        f"Created {now_iso()} by {TOOL_NAME} {TOOL_VERSION}. Status: **{status}**.",
        "",
        "## What is in here",
        "",
        f"- `manifest.json`: the reduced workload (source of truth, exact statement boundaries). "
        f"{cost[0]} session(s), {cost[1]} statement(s), {cost[2]} SQL bytes.",
        "- `sessions/*.sql`: one file per concurrent session, for reading only. The sessions must run "
        "**concurrently**, each on its own connection; concatenating them is not equivalent.",
        "- `setup/`: initial state loaded before the sessions start.",
        "- `config.json`, `environment.json`, `replay-policy.json`, `oracle.json`: how it was replayed and judged.",
        "- `validation.json`, `history.json`, `evidence/`: measured reproduction counts, accepted reductions, "
        "matched log excerpts.",
        "",
        "## Sessions",
        "",
    ]
    for fn, s in zip(session_files, w2.sessions):
        lines.append(f"- `sessions/{fn}`: `{s['id']}` database=`{s.get('database')}` "
                     f"autocommit={s.get('autocommit', 'server-default')} ({len(s['statements'])} statements)")
    lines += [
        "",
        f"Connections that finish early stay open until every session has finished (eof_policy "
        f"`{m.get('eof_policy')}`), then all are closed. No implicit COMMIT is issued at end of stream.",
        "",
        "## Setup",
        "",
        f"Kind: `{setup['kind']}`. Databases created first: {setup.get('create_databases')}.",
    ]
    if setup["kind"] == "sql":
        lines.append("`setup/setup.sql` is loaded with the mysql command-line client (DELIMITER is handled there).")
    lines += ["", "## Replay", "", "```",
              f"python3 pstress_reducer.py replay --workload {shlex.quote(str(out_dir))} "
              f"--config {shlex.quote(str(out_dir / 'config.json'))} --workdir /tmp/replay-work --attempts 10",
              "```", "",
              "Adjust `server.basedir`/`server.mysqld` in config.json if the binaries live elsewhere; the "
              "environment fingerprint in environment.json records what was used.", ""]
    if portable:
        lines.append("This bundle is portable apart from the server binary (see environment.json).")
    else:
        lines.append("This bundle is **not** fully portable. It still requires:")
        lines += [f"- {x}" for x in missing]
    lines += ["", "## Validation", ""]
    if baseline:
        lines.append(f"- Unreduced baseline: {json.dumps(baseline)}")
    if validation:
        lines.append(f"- Final validation of this candidate: {json.dumps(validation)}")
    lines += ["", "Reproduction counts are heuristic checks, not statistical proofs. No claim of global "
              "minimality is made.", ""]
    lim = m.get("capture", {}).get("limitations", [])
    if lim or notes:
        lines += ["## Import limitations and notes", ""] + [f"- {x}" for x in lim + (notes or [])] + [""]
    atomic_write_text(out_dir / "README.md", "\n".join(lines))
    return out_dir
