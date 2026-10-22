"""One bounded attempt, and the repeated-attempt policies built on it.

Attempt lifecycle:
  prepare fresh trial -> restore state -> start server -> readiness -> load setup
  -> connect sessions -> replay -> collect/classify evidence -> cleanup
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

from . import oracle as O
from .config import environment_fingerprint, oracle_hash, scheduling_policy
from .lifecycle import ManagedServer, ensure_template, load_setup, run_hook, server_version
from .manifest import Candidate, Workload
from .replay import merged_timeline, run_replay
from .util import (ReducerError, append_jsonl, atomic_write_json, log, read_json, safe_rmtree, sha256_file,
                   sha256_obj, sha256_tree)


class BudgetExhausted(Exception):
    pass


class InfraFailure(Exception):
    pass


@dataclass
class RunContext:
    workload: Workload
    cfg: dict
    password: str
    workdir: Path
    version: str = ""
    source_datadir: Path | None = None
    env_fp: dict = field(default_factory=dict)
    attempts_total: int = 0
    seconds_used_before: float = 0.0
    started: float = field(default_factory=time.monotonic)
    consecutive_infra: int = 0
    keep_targets: list = field(default_factory=list)
    keep_others: list = field(default_factory=list)
    budget_enforced: bool = True

    @property
    def journal(self) -> Path:
        return self.workdir / "journal.jsonl"

    def elapsed(self) -> float:
        return self.seconds_used_before + (time.monotonic() - self.started)

    def base_key(self) -> dict:
        """Everything except the candidate itself that determines trial outcomes."""
        return {"workload": self.workload.hash, "setup": self.workload.m["setup"].get("sha256"),
                "env": self.env_fp, "oracle": oracle_hash(self.cfg),
                "scheduling": scheduling_policy(self.cfg, self.workload.m)}

    def base_key_hash(self) -> str:
        return sha256_obj(self.base_key())


def make_context(w: Workload, cfg: dict, password: str, workdir: Path) -> RunContext:
    srv = cfg["server"]
    version = server_version(srv["mysqld"])
    ctx = RunContext(workload=w, cfg=cfg, password=password, workdir=workdir, version=version)
    setup = w.m["setup"]
    template_hash = None
    if setup["kind"] == "snapshot":
        src = Path(setup["path"])
        if not src.is_dir():
            raise ReducerError(f"setup snapshot {src} is missing", 4)
        mode = setup.get("checksum_mode", "content")
        actual = sha256_tree(src, mode)
        if setup.get("sha256") and actual != setup["sha256"]:
            raise ReducerError(f"setup snapshot {src} changed since import (checksum mismatch); refusing to "
                               "use a modified snapshot", 4)
        ctx.source_datadir = src
        template_hash = actual
    else:
        ctx.source_datadir = ensure_template(cfg, workdir)
        template_hash = sha256_tree(ctx.source_datadir, "metadata") if srv["template_datadir"] else "workdir-template"
    ctx.env_fp = environment_fingerprint(cfg, version, sha256_file(srv["mysqld"]), template_hash)
    return ctx


# ---------------------------------------------------------------------------
# One attempt
# ---------------------------------------------------------------------------
def _probe(server: ManagedServer, cfg: dict, password: str):
    pr = cfg["oracle"]["probe"]
    try:
        c = server.connect(password, database=pr.get("database"), timeout=cfg["trial"]["connect_timeout"],
                           read_timeout=cfg["trial"]["probe_timeout"])
    except Exception as e:  # noqa: BLE001
        return "error", f"connect: {e}"
    try:
        cur = c.cursor()
        cur.execute(pr["query"])
        rows = cur.fetchall() if cur.description else []
        return "ok", "\n".join("\t".join("NULL" if v is None else str(v) for v in r) for r in rows)
    except Exception as e:  # noqa: BLE001
        return "error", str(e)
    finally:
        try:
            c.close()
        except Exception:  # noqa: BLE001
            pass


def run_attempt(ctx: RunContext, cand: Candidate, purpose: str) -> dict:
    cfg = ctx.cfg
    tcfg = cfg["trial"]
    srv = cfg["server"]
    ctx.attempts_total += 1
    n = ctx.attempts_total
    adir = ctx.workdir / "trials" / f"{n:06d}"
    if adir.exists():
        safe_rmtree(adir, ctx.workdir)
    adir.mkdir(parents=True)
    server = ManagedServer(cfg, adir, ctx.workdir, ctx.version)
    ev: dict = {"segments": {}, "session_errors": []}
    rr = None
    verdict: O.Verdict | None = None
    t0 = time.monotonic()
    hook_env = {"PRD_ATTEMPT_DIR": adir, "PRD_DATADIR": server.datadir, "PRD_SOCKET": server.socket,
                "PRD_ERROR_LOG": server.error_log}
    try:
        server.phase = "prepare"
        server.prepare(ctx.source_datadir)
        if ctx.cfg["hooks"]["prepare"]:
            st, why = run_hook(ctx.cfg["hooks"]["prepare"], env_extra=hook_env, timeout=cfg["hooks"]["timeout"],
                               out_path=adir / "hook-prepare.out")
            if st != "ok":
                ev["infra_error"] = f"prepare hook: {why}"
        if not ev.get("infra_error"):
            server.phase = "startup"
            server.start()
            ready, why = server.wait_ready(ctx.password, srv["startup_timeout"])
            server.mark("ready")
            if not ready:
                ev["infra_error"] = f"startup failed: {why}"
        if not ev.get("infra_error") and cfg["hooks"]["post_start"]:
            st, why = run_hook(cfg["hooks"]["post_start"], env_extra=hook_env, timeout=cfg["hooks"]["timeout"],
                               out_path=adir / "hook-post-start.out")
            if st != "ok":
                ev["infra_error"] = f"post_start hook: {why}"
        if not ev.get("infra_error"):
            server.phase = "setup"
            ok, why = load_setup(server, ctx.workload.m["setup"], cfg, ctx.password, tcfg["setup_timeout"])
            if ok and not server.alive():
                ok, why = False, "server terminated during setup"
            server.mark("setup_done")
            if not ok:
                ev["infra_error"] = f"setup failed: {why}"
        if not ev.get("infra_error"):
            server.phase = "replay"
            rr = run_replay(ctx.workload, cand, socket=str(server.socket), user=srv["user"],
                            password=ctx.password, attempt_dir=adir, trial_cfg=tcfg,
                            server_alive=server.alive)
            # Give a dying server time to finish writing its crash report.
            # Stop waiting as soon as the server answers a fresh connection (e.g. only one
            # connection was killed).
            if rr.transport_errors or rr.connect_failed or not server.alive():
                deadline = time.monotonic() + tcfg["post_crash_wait"]
                while server.alive() and time.monotonic() < deadline:
                    try:
                        server.connect(ctx.password, timeout=2, read_timeout=5).close()
                        break
                    except Exception:  # noqa: BLE001 - not answering: maybe still crashing
                        time.sleep(0.2)
            server.poll()
            server.mark("replay_done")
            for sid, o in rr.sessions.items():
                for e in o.errors:
                    ev["session_errors"].append(f"{sid} {e['id']} {e['errno']} {e['msg']}")
            ev["timed_out"] = rr.timed_out
            ev["invalid_sql_error"] = rr.invalid_sql_error
            if rr.connect_failed and server.alive():
                ev["infra_error"] = f"session connect failed: {rr.infra_error}"
            elif rr.infra_error and not rr.connect_failed and server.alive():
                ev["infra_error"] = rr.infra_error
        # evaluate oracles that need the live server before stopping it
        kind = cfg["oracle"]["kind"]
        if kind in ("sql_probe", "checker"):
            ev["server_exit"] = _exit_json(server)
            ev["segments"] = _segments(server, final=False)
            verdict = O.evaluate(cfg["oracle"], ev, run_probe=lambda: _probe(server, cfg, ctx.password),
                                 attempt_dir=adir, checker_env=hook_env)
        server.phase = "shutdown"
        server.stop(ctx.password, srv["cleanup"], srv["shutdown_timeout"])
        server.mark("end")
        ev["segments"] = _segments(server, final=True)
        ev["server_exit"] = _exit_json(server)
        if verdict is None:
            verdict = O.evaluate(cfg["oracle"], ev, attempt_dir=adir, checker_env=hook_env)
    finally:
        keep_dd = bool(verdict and verdict.outcome == O.TARGET and cfg["retention"]["keep_datadirs"])
        server.cleanup(keep_datadir=keep_dd)

    rec = {
        "attempt": n, "purpose": purpose, "candidate": cand.hash, "outcome": verdict.outcome,
        "detail": verdict.detail, "infra": verdict.infra, "other_failure": verdict.other_failure,
        "server_exit": ev.get("server_exit"), "offsets": server.offsets,
        "replay": rr.to_json() if rr else None, "seconds": round(time.monotonic() - t0, 2),
        "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    atomic_write_json(adir / "result.json", {**rec, "excerpts": verdict.excerpts})
    if verdict.outcome == O.TARGET:
        evd = ctx.workdir / "evidence" / cand.hash[:16]
        evd.mkdir(parents=True, exist_ok=True)
        atomic_write_json(evd / f"attempt-{n:06d}.json",
                          {"attempt": n, "detail": verdict.detail, "excerpts": verdict.excerpts,
                           "server_exit": ev.get("server_exit"),
                           "last_events": merged_timeline(adir, limit=40)})
    append_jsonl(ctx.journal, {"type": "attempt", **{k: v for k, v in rec.items() if k != "replay"},
                               "sent": {k: v["sent"] for k, v in (rec["replay"] or {}).get("sessions", {}).items()}})
    _retain(ctx, adir, verdict.outcome == O.TARGET)
    log(f"attempt {n} [{purpose}] {verdict.outcome} {verdict.detail[:160]} ({rec['seconds']}s)")
    rec["last_sent"] = {k: v["last_sent"] for k, v in (rec["replay"] or {}).get("sessions", {}).items()}
    return rec


def _exit_json(server: ManagedServer):
    x = server.poll() if server.exit is None else server.exit
    if x is None:
        return None
    return {"returncode": x.returncode, "signal": x.signal, "expected": x.expected, "phase": server.exit_phase}


def _segments(server: ManagedServer, final: bool) -> dict:
    o = server.offsets
    ready = o.get("ready", 0)
    setup = o.get("setup_done", ready)
    replay = o.get("replay_done", setup)
    end = o.get("end") if final else None
    return {"startup": server.read_segment(0, ready),
            "setup": server.read_segment(ready, setup),
            "replay": server.read_segment(setup, replay),
            "shutdown": server.read_segment(replay, end) if final else ""}


def _retain(ctx: RunContext, adir: Path, target: bool) -> None:
    r = ctx.cfg["retention"]
    lst, limit = (ctx.keep_targets, r["keep_target_trials"]) if target else (ctx.keep_others, r["keep_other_trials"])
    lst.append(adir)
    while len(lst) > max(0, limit):
        old = lst.pop(0)
        if old.exists():
            safe_rmtree(old, ctx.workdir)


# ---------------------------------------------------------------------------
# Repeated-attempt policies
# ---------------------------------------------------------------------------
@dataclass
class Stats:
    target: int = 0
    no_target: int = 0
    invalid: int = 0
    unresolved: int = 0
    other_failures: int = 0
    infra: int = 0
    last_target_attempt: dict | None = None

    @property
    def valid(self) -> int:
        return self.target + self.no_target

    @property
    def rate(self) -> float | None:
        return self.target / self.valid if self.valid else None

    def add(self, rec: dict) -> None:
        o = rec["outcome"]
        if o == O.TARGET:
            self.target += 1
            self.last_target_attempt = rec
        elif o == O.NO_TARGET:
            self.no_target += 1
            if rec.get("other_failure"):
                self.other_failures += 1
        elif o == O.INVALID:
            self.invalid += 1
            if rec.get("infra"):
                self.infra += 1
        else:
            self.unresolved += 1

    def to_json(self) -> dict:
        return {"target": self.target, "no_target": self.no_target, "invalid": self.invalid,
                "unresolved": self.unresolved, "other_failures": self.other_failures, "valid": self.valid,
                "hit_rate": None if self.rate is None else round(self.rate, 4)}


class Runner:
    """Runs attempts under global budgets and the infrastructure-failure circuit breaker."""

    def __init__(self, ctx: RunContext):
        self.ctx = ctx

    def check_budget(self) -> None:
        p = self.ctx.cfg["policy"]
        if not self.ctx.budget_enforced:
            return
        if self.ctx.attempts_total >= p["max_total_attempts"]:
            raise BudgetExhausted(f"max_total_attempts={p['max_total_attempts']} reached")
        if self.ctx.elapsed() >= p["max_seconds"]:
            raise BudgetExhausted(f"max_seconds={p['max_seconds']} reached")

    def attempt(self, cand: Candidate, purpose: str) -> dict:
        self.check_budget()
        rec = run_attempt(self.ctx, cand, purpose)
        if rec["outcome"] == O.INVALID and rec.get("infra"):
            self.ctx.consecutive_infra += 1
            lim = self.ctx.cfg["policy"]["max_consecutive_infra_failures"]
            if self.ctx.consecutive_infra >= lim:
                raise InfraFailure(f"{self.ctx.consecutive_infra} consecutive infrastructure failures; last: "
                                   f"{rec['detail']}")
        else:
            self.ctx.consecutive_infra = 0
        return rec

    def measure(self, cand: Candidate, *, valid_attempts: int, required_hits: int | None, purpose: str,
                stop_early: bool) -> Stats:
        """Up to `valid_attempts` valid attempts.  With stop_early, stop once `required_hits` is
        reached or can no longer be reached.  Invalid/unresolved attempts are capped too."""
        st = Stats()
        cap_bad = max(valid_attempts, 3)
        while st.valid < valid_attempts and (st.invalid + st.unresolved) < cap_bad:
            st.add(self.attempt(cand, purpose))
            if stop_early and required_hits is not None:
                if st.target >= required_hits:
                    break
                if st.target + (valid_attempts - st.valid) < required_hits:
                    break
        return st


class Evaluator:
    """Candidate acceptance with per-round negative caching and accumulated evidence."""

    def __init__(self, runner: Runner):
        self.runner = runner
        self.ctx = runner.ctx
        self.path = self.ctx.workdir / "cache.json"
        self.cache: dict = read_json(self.path) if self.path.exists() else {}
        self.base = self.ctx.base_key_hash()

    def key(self, cand: Candidate) -> str:
        return sha256_obj({"base": self.base, "candidate": cand.hash})

    def save(self) -> None:
        atomic_write_json(self.path, self.cache, indent=None)

    def evaluate(self, cand: Candidate, round_no: int, label: str) -> tuple[bool, Stats]:
        p = self.ctx.cfg["policy"]
        k = self.key(cand)
        rec = self.cache.get(k)
        if rec and rec.get("rejected_round") == round_no:
            st = Stats(target=rec["target"], no_target=rec["no_target"])
            return False, st
        st = Stats()
        accept = False
        while True:
            r = self.runner.attempt(cand, f"candidate {label}")
            st.add(r)
            if st.target >= p["candidate_accept_hits"]:
                accept = True
                break
            if st.target + (p["candidate_max_valid"] - st.valid) < p["candidate_accept_hits"]:
                break
            if st.invalid + st.unresolved >= p["max_invalid_per_candidate"]:
                break
        tot = self.cache.setdefault(k, {"candidate": cand.hash, "target": 0, "no_target": 0, "invalid": 0,
                                        "unresolved": 0})
        tot["target"] += st.target
        tot["no_target"] += st.no_target
        tot["invalid"] += st.invalid
        tot["unresolved"] += st.unresolved
        if accept:
            tot["accepted"] = True
            tot.pop("rejected_round", None)
        else:
            tot["rejected_round"] = round_no  # a flaky negative is only trusted for this round
        self.save()
        append_jsonl(self.ctx.journal, {"type": "decision", "candidate": cand.hash, "label": label,
                                        "accepted": accept, "stats": st.to_json(), "round": round_no})
        return accept, st


def save_candidate(workdir: Path, cand: Candidate) -> Path:
    p = workdir / "candidates" / f"{cand.hash}.json"
    if not p.exists():
        atomic_write_json(p, cand.to_json(), indent=None)
    return p


def load_candidate(workdir: Path, h: str) -> Candidate:
    return Candidate.from_json(read_json(workdir / "candidates" / f"{h}.json"))
