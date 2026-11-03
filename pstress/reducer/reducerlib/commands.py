"""Command implementations: import, verify, replay, reduce, resume, export, status."""

from __future__ import annotations

import fcntl
import json
import os
import signal
import time
from pathlib import Path

from .bundle import export_bundle, load_workload
from .config import load_config, password, require_server
from .importer import (import_manifest, import_pstress_logs, import_plain_sql, render_report_text)
from .lifecycle import REGISTRY
from .manifest import Candidate, Workload
from .reducer import Reducer
from .trial import (BudgetExhausted, Evaluator, InfraFailure, RunContext, Runner, load_candidate,
                    make_context, save_candidate)
from .util import (EXIT_BASELINE_UNCONFIRMED, EXIT_BUDGET_PARTIAL, EXIT_FINAL_VALIDATION_FAILED,
                   EXIT_INTERRUPTED, EXIT_INVALID_RUNS, EXIT_OK, ReducerError,
                   atomic_write_json, atomic_write_text, log, read_json, set_log_file, warn)

STATE_VERSION = 1


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
class WorkdirLock:
    def __init__(self, workdir: Path):
        self.path = workdir / "LOCK"
        self.f = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.path, "a+")
        try:
            fcntl.flock(self.f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ReducerError(f"work directory {self.path.parent} is in use by another reducer process")
        self.f.seek(0)
        self.f.truncate()
        self.f.write(f"{os.getpid()}\n")
        self.f.flush()
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self.f, fcntl.LOCK_UN)
            self.f.close()
        except Exception:  # noqa: BLE001
            pass


def _install_sigterm():
    def handler(signum, frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, handler)


def _cfg(args, workdir_required=True) -> dict:
    cfg = load_config(args.config, overrides={"workdir": getattr(args, "workdir", None)})
    if workdir_required and not cfg["workdir"]:
        raise ReducerError("a work directory is required (--workdir or \"workdir\" in the config)")
    if cfg["workdir"]:
        cfg["workdir"] = str(Path(cfg["workdir"]).resolve())
    return cfg


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


# ---------------------------------------------------------------------------
# import
# ---------------------------------------------------------------------------
def cmd_import(args) -> int:
    out = Path(args.out)
    if out.exists() and any(out.iterdir()) and not args.force:
        raise ReducerError(f"output directory {out} is not empty (use --force to overwrite)")
    out.mkdir(parents=True, exist_ok=True)
    autocommit = {"server-default": "server-default", "on": True, "off": False}[args.autocommit]
    cdbs = args.create_database if args.create_database else None
    try:
        if args.manifest:
            manifest, report = import_manifest(Path(args.manifest))
        elif args.sql_file:
            manifest, report = import_plain_sql([Path(p) for p in args.sql_file], setup_spec=args.setup,
                                                database=args.database, create_databases=cdbs,
                                                loader=args.setup_loader, snapshot_checksum=args.snapshot_checksum,
                                                external_files=args.external_file or [], autocommit=autocommit)
        else:
            if not args.setup:
                raise ReducerError("an explicit --setup is required (none | sql:<file> | snapshot:<datadir>)")
            manifest, report = import_pstress_logs(
                Path(args.incident_dir), step=args.step, node=args.node, setup_spec=args.setup,
                database=args.database, create_databases=cdbs, loader=args.setup_loader,
                snapshot_checksum=args.snapshot_checksum, external_files=args.external_file or [],
                include_failed=not args.exclude_failed, include_incomplete=args.include_incomplete,
                skip_unframed=args.skip_unframed, allow_empty_setup=args.allow_empty_setup,
                pstress_revision=args.pstress_revision, autocommit=autocommit,
                duration_unit=args.duration_unit)
    except ReducerError as e:
        rep = getattr(e, "report", None)
        if rep is not None:
            atomic_write_json(out / "import-report.json", rep)
            atomic_write_text(out / "import-report.txt", render_report_text(rep, None))
            print(render_report_text(rep, None))
        raise
    if args.pin_use:
        for s in manifest["sessions"]:
            for st in s["statements"][:1]:
                if st["sql"].strip().upper().startswith("USE "):
                    st["pinned"] = True
    atomic_write_json(out / "manifest.json", manifest)
    atomic_write_json(out / "import-report.json", report)
    text = render_report_text(report, manifest)
    atomic_write_text(out / "import-report.txt", text)
    w = Workload(manifest)
    (out / "sessions").mkdir(exist_ok=True)
    for i, s in enumerate(w.sessions):
        safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in s["id"])
        atomic_write_text(out / "sessions" / f"{i:02d}_{safe}.sql", w.session_sql_text(s["id"]))
    print(text, end="")
    print(f"Wrote {out / 'manifest.json'} (workload sha256 {w.hash[:16]}…)")
    return EXIT_OK


# ---------------------------------------------------------------------------
# verify / replay
# ---------------------------------------------------------------------------
def _measure_cmd(args, attempts: int, purpose: str, keep_all: bool) -> int:
    cfg = _cfg(args)
    require_server(cfg)
    w, _ = load_workload(args.workload)
    workdir = Path(cfg["workdir"])
    if (workdir / "state.json").exists():
        workdir = workdir / f"{purpose}-{time.strftime('%Y%m%d-%H%M%S')}"
    workdir.mkdir(parents=True, exist_ok=True)
    if keep_all:
        cfg["retention"]["keep_other_trials"] = max(cfg["retention"]["keep_other_trials"], attempts)
        cfg["retention"]["keep_target_trials"] = max(cfg["retention"]["keep_target_trials"], attempts)
    set_log_file(workdir / "reducer.log")
    _install_sigterm()
    with WorkdirLock(workdir):
        pw = password(cfg)
        ctx = make_context(w, cfg, pw, workdir)
        ctx.budget_enforced = False
        runner = Runner(ctx)
        cand = w.full_candidate()
        if getattr(args, "candidate", None):
            cand = load_candidate(Path(cfg["workdir"]), args.candidate)
            cand.validate_against(w)
        try:
            st = runner.measure(cand, valid_attempts=attempts, required_hits=None, purpose=purpose,
                                stop_early=False)
        except KeyboardInterrupt:
            REGISTRY.kill_all()
            warn("interrupted")
            return EXIT_INTERRUPTED
        except InfraFailure as e:
            warn(str(e))
            return EXIT_INVALID_RUNS
        rep = {"purpose": purpose, "candidate": cand.hash, "cost": w.cost(cand), "stats": st.to_json(),
               "server_version": ctx.version, "workdir": str(workdir)}
        atomic_write_json(workdir / f"{purpose}-report.json", rep)
        _print(rep)
        if st.target == 0:
            print("Target NOT observed within the attempt budget (unconfirmed, not impossible).")
            return EXIT_BASELINE_UNCONFIRMED
        return EXIT_OK


def cmd_verify(args) -> int:
    cfg_attempts = args.attempts
    if cfg_attempts is None:
        cfg_attempts = _cfg(args)["policy"]["baseline_attempts"]
    return _measure_cmd(args, cfg_attempts, "verify", keep_all=False)


def cmd_replay(args) -> int:
    return _measure_cmd(args, args.attempts or 1, "replay", keep_all=True)


# ---------------------------------------------------------------------------
# reduce / resume
# ---------------------------------------------------------------------------
def _persist(workdir: Path, state: dict, ctx: RunContext | None) -> None:
    if ctx is not None:
        state["attempts_total"] = ctx.attempts_total
        state["seconds_used"] = round(ctx.elapsed(), 1)
    state["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    atomic_write_json(workdir / "state.json", state)


def cmd_reduce(args) -> int:
    cfg = _cfg(args)
    require_server(cfg)
    w, mpath = load_workload(args.workload)
    workdir = Path(cfg["workdir"])
    if (workdir / "state.json").exists():
        raise ReducerError(f"{workdir} already holds a reduction; use 'resume' or choose another --workdir")
    workdir.mkdir(parents=True, exist_ok=True)
    set_log_file(workdir / "reducer.log")
    _install_sigterm()
    with WorkdirLock(workdir):
        atomic_write_json(workdir / "workload.json", w.m)
        atomic_write_json(workdir / "config.effective.json", cfg)
        pw = password(cfg)
        ctx = make_context(w, cfg, pw, workdir)
        full = w.full_candidate()
        save_candidate(workdir, full)
        state = {"state_version": STATE_VERSION, "status": "baseline", "workload_sha256": w.hash,
                 "workload_source": str(mpath), "base_key": ctx.base_key(), "base_key_hash": ctx.base_key_hash(),
                 "attempts_total": 0, "seconds_used": 0.0, "baseline": None, "empty_probe": None,
                 "best": full.hash, "history": [], "tail_hints": {}, "reducer": {}, "final": [],
                 "budget_exhausted": None, "created_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
        _persist(workdir, state, ctx)
        log(f"reduce: {len(w.sessions)} sessions, {full.statement_count()} statements, cost {w.cost(full)}, "
            f"server {ctx.version}")
        return _drive(ctx, state, args)


def cmd_resume(args) -> int:
    workdir = Path(args.workdir).resolve()
    sp = workdir / "state.json"
    if not sp.exists():
        raise ReducerError(f"no reduction state in {workdir}")
    set_log_file(workdir / "reducer.log")
    _install_sigterm()
    with WorkdirLock(workdir):
        state = read_json(sp)
        if state.get("state_version") != STATE_VERSION:
            raise ReducerError("state file version not supported")
        if args.config:
            cfg = load_config(args.config, overrides={"workdir": str(workdir)})
        else:
            cfg = read_json(workdir / "config.effective.json")
        cfg["workdir"] = str(workdir)
        require_server(cfg)
        w, _ = load_workload(workdir / "workload.json")
        if w.hash != state["workload_sha256"]:
            raise ReducerError("workload.json in the work directory was modified; refusing to resume")
        pw = password(cfg)
        ctx = make_context(w, cfg, pw, workdir)
        ctx.attempts_total = state["attempts_total"]
        ctx.seconds_used_before = state["seconds_used"]
        if state["status"] in ("verified", "fell-back", "final-validation-failed", "baseline-unconfirmed",
                               "target-without-sql", "partial-budget-exhausted"):
            if not args.extend:
                print(f"Reduction already finished with status {state['status']}; nothing to resume "
                      "(use --extend with a larger budget to continue reducing).")
                return EXIT_OK
            if state["status"] == "baseline-unconfirmed":
                state["baseline"] = None
                state["status"] = "baseline"
            else:
                state["status"] = "reducing"
                state["final"] = []
                if state.get("best_unvalidated"):
                    state["best"] = state.pop("best_unvalidated")
            state["budget_exhausted"] = None
        if ctx.base_key_hash() != state["base_key_hash"]:
            old, new = state["base_key"], ctx.base_key()
            diff = sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))
            if not args.revalidate:
                raise ReducerError(f"environment/policy changed since the reduction started ({', '.join(diff)}); "
                                   "evidence is not reused across environments. Re-run with --revalidate to "
                                   "re-verify the best candidate under the new environment first.")
            best = load_candidate(workdir, state["best"])
            runner = Runner(ctx)
            log(f"revalidating best candidate {best.hash[:12]} under changed {diff}")
            st = runner.measure(best, valid_attempts=cfg["policy"]["baseline_attempts"], required_hits=None,
                                purpose="revalidate", stop_early=False)
            state["history"].append({"candidate": best.hash, "cost": list(w.cost(best)), "stats": st.to_json(),
                                     "label": f"revalidated after change of {diff}",
                                     "time": time.strftime("%Y-%m-%dT%H:%M:%S")})
            if st.target == 0:
                _persist(workdir, state, ctx)
                print("Best candidate does not reproduce under the new environment; not resuming.")
                return EXIT_BASELINE_UNCONFIRMED
            state["base_key"], state["base_key_hash"] = new, ctx.base_key_hash()
            state["reducer"]["round"] = state["reducer"].get("round", 1) + 1  # fresh negative cache
            atomic_write_json(workdir / "config.effective.json", cfg)
        log(f"resuming at attempt {ctx.attempts_total}, status {state['status']}, cursor {state.get('reducer')}")
        return _drive(ctx, state, args)


def _drive(ctx: RunContext, state: dict, args) -> int:
    w, cfg, workdir = ctx.workload, ctx.cfg, ctx.workdir
    pol = cfg["policy"]
    runner = Runner(ctx)
    evaluator = Evaluator(runner)
    full = w.full_candidate()
    persist = lambda: _persist(workdir, state, ctx)  # noqa: E731
    try:
        # ---- baseline --------------------------------------------------------
        if state["baseline"] is None:
            log(f"baseline: up to {pol['baseline_attempts']} valid attempts of the unreduced workload")
            st = runner.measure(full, valid_attempts=pol["baseline_attempts"], required_hits=None,
                                purpose="baseline", stop_early=False)
            state["baseline"] = st.to_json()
            if st.last_target_attempt:
                state["tail_hints"][full.hash] = st.last_target_attempt.get("last_sent")
            if st.target == 0:
                state["status"] = "baseline-unconfirmed"
                persist()
                _write_report(ctx, state)
                print(f"Baseline: target not observed in {st.valid} valid attempts "
                      f"({st.invalid} invalid, {st.unresolved} unresolved). Not reducing an unverified "
                      "workload; increase policy.baseline_attempts or check the oracle. "
                      f"Details: {workdir / 'report.txt'}")
                return EXIT_BASELINE_UNCONFIRMED
            state["history"].append({"candidate": full.hash, "cost": list(w.cost(full)), "stats": st.to_json(),
                                     "label": "baseline", "time": time.strftime("%Y-%m-%dT%H:%M:%S")})
            state["status"] = "reducing"
            need = pol["final_required_hits"] / pol["final_attempts"]
            if st.rate is not None and st.rate < need:
                state["policy_warning"] = (
                    f"baseline hit rate {st.rate:.2f} is below the final-validation threshold "
                    f"{pol['final_required_hits']}/{pol['final_attempts']}={need:.2f}: even the unreduced workload "
                    "would probably fail final validation. Consider raising policy.final_attempts or lowering "
                    "policy.final_required_hits, and expect many false rejections during reduction.")
                warn(state["policy_warning"])
            persist()
            log(f"baseline: {st.to_json()}")
        # ---- empty-workload diagnostic ----------------------------------------
        if pol["probe_empty_workload"] and state.get("empty_probe") is None:
            empty = Candidate({})
            st = runner.measure(empty, valid_attempts=2, required_hits=1, purpose="empty-workload-probe",
                                stop_early=True)
            state["empty_probe"] = st.to_json()
            persist()
            if st.target:
                save_candidate(workdir, empty)
                state["best"] = empty.hash
                state["history"].append({"candidate": empty.hash, "cost": list(w.cost(empty)),
                                         "stats": st.to_json(), "label": "empty workload",
                                         "time": time.strftime("%Y-%m-%dT%H:%M:%S")})
                state["status"] = "target-without-sql"
                persist()
                _export(ctx, state, args, validation=st.to_json(), status="target-without-sql")
                _write_report(ctx, state)
                print("The target reproduces with NO session SQL at all (setup/startup alone). The workload "
                      "is not needed; see the bundle and report.")
                return EXIT_OK

        # ---- deletion passes -----------------------------------------------------
        if state["status"] == "reducing":
            best = load_candidate(workdir, state["best"])
            base_rate = state["baseline"].get("hit_rate") or 0

            def test(c: Candidate, round_no: int, label: str) -> bool:
                ok, st = evaluator.evaluate(c, round_no, label)
                if ok:
                    save_candidate(workdir, c)
                    state["best"] = c.hash
                    entry = {"candidate": c.hash, "cost": list(w.cost(c)), "stats": st.to_json(), "label": label,
                             "time": time.strftime("%Y-%m-%dT%H:%M:%S")}
                    state["history"].append(entry)
                    if st.last_target_attempt:
                        state["tail_hints"] = {c.hash: st.last_target_attempt.get("last_sent")}
                    rate = st.rate or 0
                    if base_rate and rate < base_rate * pol["low_hit_rate_ratio"]:
                        entry["warning"] = (f"hit rate {rate:.2f} is far below the baseline {base_rate:.2f}; "
                                            "the reduced workload may be much less reliable")
                        warn(entry["warning"])
                    log(f"ACCEPTED {label}: cost {w.cost(c)} stats {st.to_json()}")
                persist()
                return ok

            def on_cursor(rs: dict) -> None:
                state["reducer"] = rs
                persist()

            red = Reducer(w, best, test, state=state["reducer"], on_cursor=on_cursor,
                          tail_hint=lambda c: state["tail_hints"].get(c.hash),
                          max_rounds=pol["max_rounds"], transaction_blocks=pol["transaction_blocks"],
                          trim_tail=pol["trim_unexecuted_tail"])
            red.run()
            state["reducer"] = red.state
            state["status"] = "final-validation"
            persist()
    except BudgetExhausted as e:
        warn(f"budget exhausted: {e}")
        state["budget_exhausted"] = str(e)
        if state["baseline"] is None or state["status"] == "baseline":
            persist()
            _write_report(ctx, state)
            return EXIT_BUDGET_PARTIAL
        state["status"] = "final-validation"
        persist()
    except InfraFailure as e:
        warn(str(e))
        state["status_detail"] = str(e)
        persist()
        _write_report(ctx, state)
        print(f"Stopped: {e}. The last confirmed best candidate is kept ({state['best'][:12]}); fix the "
              "environment and run 'resume'.")
        return EXIT_INVALID_RUNS
    except KeyboardInterrupt:
        REGISTRY.kill_all()
        state["interrupted_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        persist()
        _write_report(ctx, state)
        print(f"Interrupted. Best confirmed candidate {state['best'][:12]} is saved; continue with: "
              f"pstress_reducer.py resume --workdir {workdir}")
        return EXIT_INTERRUPTED

    # ---- final validation (own budget, outside the global attempt budget) -----
    try:
        return _final(ctx, state, args, runner)
    except KeyboardInterrupt:
        REGISTRY.kill_all()
        persist()
        print(f"Interrupted during final validation; resume with: pstress_reducer.py resume --workdir {workdir}")
        return EXIT_INTERRUPTED
    except InfraFailure as e:
        warn(str(e))
        persist()
        return EXIT_INVALID_RUNS


def _final(ctx: RunContext, state: dict, args, runner: Runner) -> int:
    cfg, workdir = ctx.cfg, ctx.workdir
    pol = cfg["policy"]
    ctx.budget_enforced = False
    chain = []
    seen = set()
    for h in reversed(state["history"]):
        if h["candidate"] not in seen:
            chain.append(h)
            seen.add(h["candidate"])
    validated = None
    done = {f["candidate"]: f for f in state.get("final", [])}
    for i, h in enumerate(chain[: 1 + pol["final_fallbacks"]]):
        if h["candidate"] in done:
            f = done[h["candidate"]]
        else:
            cand = load_candidate(workdir, h["candidate"])
            log(f"final validation of {h['candidate'][:12]} (cost {h['cost']}): need {pol['final_required_hits']} "
                f"hits within {pol['final_attempts']} valid attempts")
            st = runner.measure(cand, valid_attempts=pol["final_attempts"], required_hits=pol["final_required_hits"],
                                purpose="final-validation", stop_early=True)
            f = {"candidate": h["candidate"], "cost": h["cost"], "stats": st.to_json(),
                 "passed": st.target >= pol["final_required_hits"]}
            state.setdefault("final", []).append(f)
            _persist(workdir, state, ctx)
        if f["passed"]:
            validated = (i, f)
            break
    if validated and validated[0] == 0:
        state["status"] = "partial-budget-exhausted" if state.get("budget_exhausted") else "verified"
        code = EXIT_BUDGET_PARTIAL if state.get("budget_exhausted") else EXIT_OK
        result = validated[1]
    elif validated:
        state["status"] = "fell-back"
        state["best_unvalidated"] = state["best"]
        state["best"] = validated[1]["candidate"]
        code = EXIT_FINAL_VALIDATION_FAILED
        result = validated[1]
    else:
        state["status"] = "final-validation-failed"
        code = EXIT_FINAL_VALIDATION_FAILED
        mine = [f for f in state.get("final", []) if f["candidate"] == state["best"]]
        result = mine[-1] if mine else None
    _persist(workdir, state, ctx)
    _export(ctx, state, args, validation=result, status=state["status"])
    _write_report(ctx, state)
    print((workdir / "report.txt").read_text(), end="")
    return code


def _qualified_claim(state: dict) -> str:
    rs = state.get("reducer") or {}
    base = state.get("baseline") or {}
    fin = [f for f in state.get("final", []) if f["candidate"] == state["best"]]
    deterministic = (base.get("hit_rate") == 1.0 and fin and fin[-1]["stats"].get("hit_rate") == 1.0)
    if state.get("budget_exhausted"):
        return "Budget exhausted: no further reduction was confirmed within budget; the result is a partial reduction."
    if rs.get("last_round_clean") and deterministic and state["status"] == "verified":
        return ("A complete final round (including every single-statement removal) found no accepted reduction, "
                "and every observed attempt reproduced: the result is statement-level 1-minimal under this "
                "replay/acceptance policy (qualified; not a global minimum).")
    return ("No further reduction was confirmed within the configured attempt budgets (flaky reproduction "
            "means this is not a minimality proof).")


def _export(ctx: RunContext, state: dict, args, validation, status: str) -> Path:
    w, cfg, workdir = ctx.workload, ctx.cfg, ctx.workdir
    best = load_candidate(workdir, state["best"])
    out = Path(getattr(args, "export", None) or (workdir / "bundle"))
    notes = []
    if state.get("budget_exhausted"):
        notes.append(f"budget exhausted: {state['budget_exhausted']}")
    if status == "fell-back":
        notes.append(f"final validation of the smallest candidate {state.get('best_unvalidated', '')[:12]} failed; "
                     "this bundle holds the previous confirmed candidate")
    if status == "final-validation-failed":
        notes.append("FINAL VALIDATION FAILED for every tried candidate: this bundle is NOT validated")
    notes.append(_qualified_claim(state))
    ev = workdir / "evidence" / best.hash[:16]
    export_bundle(out, w, best, cfg=cfg, env_fp=ctx.env_fp, history=state["history"], validation=validation,
                  baseline=state.get("baseline"), evidence_dir=ev, status=status,
                  include_snapshot=getattr(args, "include_snapshot", False), notes=notes)
    state["bundle"] = str(out)
    _persist(workdir, state, ctx)
    log(f"bundle written to {out}")
    return out


def _write_report(ctx: RunContext, state: dict) -> None:
    w, workdir = ctx.workload, ctx.workdir
    full = w.full_candidate()
    best = load_candidate(workdir, state["best"])
    rep = {
        "status": state["status"], "status_detail": state.get("status_detail"),
        "budget_exhausted": state.get("budget_exhausted"),
        "server_version": ctx.version, "attempts_total": ctx.attempts_total,
        "seconds_used": round(ctx.elapsed(), 1),
        "baseline": state.get("baseline"), "empty_probe": state.get("empty_probe"),
        "original_cost": list(w.cost(full)), "best_cost": list(w.cost(best)), "best": best.hash,
        "best_sessions": {k: len(v) for k, v in best.sessions.items()},
        "pinned": w.pinned_cost(),
        "final_validation": state.get("final"), "bundle": state.get("bundle"),
        "claim": _qualified_claim(state),
        "import_limitations": w.m.get("capture", {}).get("limitations", []),
        "accepted_reductions": len(state.get("history", [])) - 1,
    }
    atomic_write_json(workdir / "report.json", rep)
    lines = [f"Status: {rep['status']}" + (f" ({rep['status_detail']})" if rep["status_detail"] else ""),
             f"Server: {rep['server_version']}",
             f"Attempts: {rep['attempts_total']} in {rep['seconds_used']}s",
             f"Baseline: {json.dumps(rep['baseline'])}",
             f"Empty-workload probe: {json.dumps(rep['empty_probe'])}",
             f"Cost (sessions, statements, SQL bytes): {tuple(rep['original_cost'])} -> {tuple(rep['best_cost'])}",
             f"Best candidate: {best.hash[:16]}  sessions: {rep['best_sessions']}",
             f"Pinned/setup (not counted): {rep['pinned']}"]
    for f in rep["final_validation"] or []:
        lines.append(f"Final validation {f['candidate'][:12]} cost {tuple(f['cost'])}: {json.dumps(f['stats'])} "
                     f"-> {'PASSED' if f['passed'] else 'FAILED'}")
    if state.get("policy_warning"):
        lines.append(f"WARNING: {state['policy_warning']}")
    for h in state.get("history", []):
        if h.get("warning"):
            lines.append(f"WARNING at {h['label']}: {h['warning']}")
    if rep["budget_exhausted"]:
        lines.append(f"Budget exhausted: {rep['budget_exhausted']}")
    lines.append(rep["claim"])
    if rep["bundle"]:
        lines.append(f"Bundle: {rep['bundle']}")
    for lim in rep["import_limitations"]:
        lines.append(f"Import limitation: {lim}")
    atomic_write_text(workdir / "report.txt", "\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# export / status
# ---------------------------------------------------------------------------
def cmd_export(args) -> int:
    workdir = Path(args.workdir).resolve()
    state = read_json(workdir / "state.json")
    cfg = read_json(workdir / "config.effective.json")
    w, _ = load_workload(workdir / "workload.json")
    h = args.candidate or state["best"]
    cand = load_candidate(workdir, h)
    status = state["status"] if h == state["best"] else "unvalidated-export"
    fin = [f for f in state.get("final", []) if f["candidate"] == h]
    from .config import environment_fingerprint
    env_fp = state["base_key"]["env"] if "base_key" in state else environment_fingerprint(cfg, None, None, None)
    export_bundle(Path(args.out), w, cand, cfg=cfg, env_fp=env_fp, history=state["history"],
                  validation=fin[-1] if fin else None, baseline=state.get("baseline"),
                  evidence_dir=workdir / "evidence" / cand.hash[:16], status=status,
                  include_snapshot=args.include_snapshot)
    print(f"Exported {h[:12]} ({status}) to {args.out}")
    return EXIT_OK


def cmd_status(args) -> int:
    workdir = Path(args.workdir).resolve()
    state = read_json(workdir / "state.json")
    w, _ = load_workload(workdir / "workload.json")
    best = load_candidate(workdir, state["best"])
    _print({"status": state["status"], "attempts_total": state["attempts_total"],
            "seconds_used": state["seconds_used"], "baseline": state.get("baseline"),
            "original_cost": w.cost(w.full_candidate()), "best_cost": w.cost(best),
            "accepted_reductions": len(state["history"]) - 1, "cursor": state.get("reducer"),
            "budget_exhausted": state.get("budget_exhausted"), "final": state.get("final")})
    return EXIT_OK

