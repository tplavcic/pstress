"""Replay coordinator: one killable worker process per session, a common start gate,
an overall deadline, and on-disk event logs.  Sessions are never gated per statement,
so a session blocked on a lock does not prevent another one from committing."""

from __future__ import annotations

import json
import os
import selectors
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .lifecycle import REGISTRY, kill_group
from .manifest import Candidate, Workload
from .util import iter_jsonl

PKG_PARENT = str(Path(__file__).resolve().parent.parent)


@dataclass
class SessionOutcome:
    session_id: str
    state: str = "starting"        # starting|ready|connect_failed|done|aborted|killed|exited
    detail: str = ""
    sent: int = 0
    completed: int = 0
    errors: list = field(default_factory=list)   # [{"id","errno","msg","class"}]
    last_sent: str | None = None
    last_completed: str | None = None
    unanswered: str | None = None                # sent but never completed (in flight at end)


@dataclass
class ReplayResult:
    sessions: dict
    connect_failed: bool = False
    timed_out: bool = False
    infra_error: str | None = None
    invalid_sql_error: str | None = None
    transport_errors: int = 0
    started_at: float = 0.0
    ended_at: float = 0.0

    def to_json(self) -> dict:
        return {
            "connect_failed": self.connect_failed, "timed_out": self.timed_out,
            "infra_error": self.infra_error, "invalid_sql_error": self.invalid_sql_error,
            "transport_errors": self.transport_errors,
            "duration": round(self.ended_at - self.started_at, 3) if self.ended_at else None,
            "sessions": {k: vars(v) for k, v in self.sessions.items()},
        }


def run_replay(w: Workload, cand: Candidate, *, socket: str, user: str, password: str, attempt_dir: Path,
               trial_cfg: dict, server_alive: Callable[[], bool]) -> ReplayResult:
    rdir = attempt_dir / "replay"
    rdir.mkdir(parents=True, exist_ok=True)
    order = [s["id"] for s in w.sessions if cand.sessions.get(s["id"])]
    outcomes = {sid: SessionOutcome(sid) for sid in order}
    result = ReplayResult(sessions=outcomes)
    if not order:
        result.started_at = result.ended_at = time.monotonic()
        return result

    env = dict(os.environ)
    env["PYTHONPATH"] = PKG_PARENT + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PRD_PASSWORD"] = password
    procs: dict[str, subprocess.Popen] = {}
    files = {}
    sel = selectors.DefaultSelector()
    bufs: dict[str, bytes] = {}
    eof_policy = w.m.get("eof_policy", "hold-until-all-finished")
    try:
        for idx, sid in enumerate(order):
            s = w.session_by_id[sid]
            keep = set(cand.sessions[sid])
            stmt_file = rdir / f"s{idx}.statements.jsonl"
            with open(stmt_file, "w", encoding="utf-8") as f:
                for st in s["statements"]:
                    if st["id"] in keep:
                        f.write(json.dumps({"id": st["id"], "sql": st["sql"]}, ensure_ascii=True) + "\n")
            spec = {
                "session_id": sid, "socket": socket, "user": user,
                "database": s.get("database"), "autocommit": s.get("autocommit", "server-default"),
                "charset": s.get("charset", "utf8mb4"),
                "connect_timeout": trial_cfg["connect_timeout"], "query_timeout": trial_cfg["query_timeout"],
                "statements_file": str(stmt_file), "events_file": str(rdir / f"s{idx}.events.jsonl"),
                "sql_error_policy": trial_cfg["sql_error_policy"], "eof_policy": eof_policy,
            }
            spec_file = rdir / f"s{idx}.spec.json"
            spec_file.write_text(json.dumps(spec, indent=1))
            errf = open(rdir / f"s{idx}.stderr", "wb")
            files[sid] = (idx, errf)
            p = subprocess.Popen([sys.executable, "-m", "reducerlib.worker", str(spec_file)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errf, env=env,
                                 cwd=str(attempt_dir), start_new_session=True)
            REGISTRY.add(p, f"worker {sid}")
            procs[sid] = p
            os.set_blocking(p.stdout.fileno(), False)
            sel.register(p.stdout, selectors.EVENT_READ, sid)
            bufs[sid] = b""

        def pump(timeout: float) -> None:
            for key, _ in sel.select(timeout):
                sid = key.data
                try:
                    chunk = os.read(key.fileobj.fileno(), 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    sel.unregister(key.fileobj)
                    o = outcomes[sid]
                    if o.state in ("starting", "ready"):
                        o.state = "exited"
                    continue
                bufs[sid] += chunk
                while b"\n" in bufs[sid]:
                    line, bufs[sid] = bufs[sid].split(b"\n", 1)
                    handle(sid, line.decode("utf-8", "replace"))

        def handle(sid: str, line: str) -> None:
            o = outcomes[sid]
            if line == "READY":
                o.state = "ready"
            elif line.startswith("CONNECT_FAILED"):
                o.state = "connect_failed"
                o.detail = line[len("CONNECT_FAILED "):]
            elif line == "DONE":
                o.state = "done"
            elif line.startswith("ABORTED"):
                o.state = "aborted"
                o.detail = line[len("ABORTED "):]

        # ---- connection phase ------------------------------------------------
        deadline = time.monotonic() + trial_cfg["connect_timeout"] + 10
        while time.monotonic() < deadline:
            if all(outcomes[s].state != "starting" for s in order):
                break
            pump(0.1)
        bad = [s for s in order if outcomes[s].state != "ready"]
        if bad:
            result.connect_failed = True
            result.infra_error = "; ".join(f"{s}: {outcomes[s].state} {outcomes[s].detail}" for s in bad)
            return result

        # ---- start gate --------------------------------------------------------
        result.started_at = time.monotonic()
        for sid in order:
            try:
                procs[sid].stdin.write(b"GO\n")
                procs[sid].stdin.flush()
            except BrokenPipeError:
                pass

        # ---- run ---------------------------------------------------------------
        deadline = result.started_at + trial_cfg["replay_timeout"]
        while True:
            pending = [s for s in order if outcomes[s].state == "ready"]
            if not pending:
                break
            if time.monotonic() >= deadline:
                result.timed_out = True
                break
            pump(0.1)
            for s in pending:
                if procs[s].poll() is not None and outcomes[s].state == "ready":
                    pump(0)
                    if outcomes[s].state == "ready":
                        outcomes[s].state = "exited"
                        outcomes[s].detail = f"worker exited rc={procs[s].returncode}"
        result.ended_at = time.monotonic()

        # ---- release held connections ---------------------------------------
        for sid in order:
            try:
                procs[sid].stdin.write(b"CLOSE\n")
                procs[sid].stdin.flush()
                procs[sid].stdin.close()
            except (BrokenPipeError, OSError):
                pass
        close_deadline = time.monotonic() + 10
        for sid in order:
            p = procs[sid]
            remaining = max(0.0, close_deadline - time.monotonic())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                pass
        return result
    finally:
        for sid, p in procs.items():
            if p.poll() is None:
                kill_group(p, f"worker {sid}")
                if outcomes[sid].state == "ready":
                    outcomes[sid].state = "killed"
            REGISTRY.discard(p)
            for stream in (p.stdin, p.stdout):
                try:
                    stream.close()
                except Exception:  # noqa: BLE001 - e.g. BrokenPipe on a worker that already exited
                    pass
        for sid, (idx, errf) in files.items():
            errf.close()
        sel.close()
        _collect_events(result, order, rdir)


def _collect_events(result: ReplayResult, order: list[str], rdir: Path) -> None:
    for idx, sid in enumerate(order):
        o = result.sessions[sid]
        open_send = None
        for e in iter_jsonl(rdir / f"s{idx}.events.jsonl"):
            if e.get("ev") == "send":
                o.sent += 1
                o.last_sent = e["id"]
                open_send = e["id"]
            elif e.get("ev") == "done":
                o.completed += 1
                o.last_completed = e["id"]
                open_send = None
                if not e.get("ok"):
                    o.errors.append({k: e.get(k) for k in ("id", "errno", "msg", "class")})
                    if e.get("class") in ("transport", "timeout"):
                        result.transport_errors += 1
                    if e.get("class") == "driver" and not result.infra_error:
                        result.infra_error = f"driver error in {sid}: {e.get('msg')}"
            elif e.get("ev") == "abort" and str(e.get("reason", "")).startswith("invalid-sql-error"):
                result.invalid_sql_error = f"{sid}: {e['reason']}"
        o.unanswered = open_send


def merged_timeline(attempt_dir: Path, limit: int = 0) -> list[dict]:
    """All worker events of an attempt merged by monotonic time (actual replay order)."""
    rdir = attempt_dir / "replay"
    out = []
    for p in sorted(rdir.glob("s*.events.jsonl")):
        idx = p.name.split(".")[0]
        for e in iter_jsonl(p):
            e["session"] = idx
            out.append(e)
    out.sort(key=lambda e: e.get("t", 0))
    return out[-limit:] if limit else out
