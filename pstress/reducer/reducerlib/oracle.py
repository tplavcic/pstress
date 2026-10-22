"""Failure oracle: classify one attempt as TARGET / NO_TARGET / INVALID / UNRESOLVED.

Only evidence produced by the current attempt is inspected, cut into labelled phases of
the attempt's own error log (startup, setup, replay, shutdown).  A kill issued by the
reducer is never a crash; an unrelated crash is recorded as NO_TARGET/other-crash and
never satisfies a specific target.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from .lifecycle import REGISTRY, kill_group

TARGET = "TARGET"
NO_TARGET = "NO_TARGET"
INVALID = "INVALID"
UNRESOLVED = "UNRESOLVED"


@dataclass
class Verdict:
    outcome: str
    detail: str = ""
    infra: bool = False            # INVALID caused by environment rather than by the candidate
    excerpts: list = field(default_factory=list)
    other_failure: str | None = None

    def to_json(self) -> dict:
        return {"outcome": self.outcome, "detail": self.detail, "infra": self.infra,
                "excerpts": self.excerpts, "other_failure": self.other_failure}


def _compile(rx: str) -> re.Pattern:
    return re.compile(rx, re.MULTILINE)


def match_all(text: str, required: list[str], excluded: list[str]) -> tuple[bool, list[dict], str | None]:
    """All required regexes must match and no excluded regex may match."""
    excerpts = []
    for rx in required:
        m = _compile(rx).search(text)
        if not m:
            return False, excerpts, None
        a = text.rfind("\n", 0, max(0, m.start() - 1))
        a = text.rfind("\n", 0, max(0, a - 1)) if a > 0 else a
        b = text.find("\n", m.end())
        b = text.find("\n", b + 1) if b != -1 else -1
        excerpts.append({"pattern": rx, "match": m.group(0)[:500],
                         "context": text[max(0, a + 1):(b if b != -1 else len(text))][:4000]})
    for rx in excluded:
        if _compile(rx).search(text):
            return False, excerpts, rx
    return True, excerpts, None


CRASH_HINT = re.compile(r"(got signal \d+|Assertion failure|InnoDB: Assertion|terminate called|"
                        r"AddressSanitizer|UndefinedBehaviorSanitizer|Segmentation fault|stack_bottom)")


def evaluate(oracle: dict, ev: dict, *, run_probe=None, attempt_dir: Path | None = None,
             checker_env: dict | None = None) -> Verdict:
    """ev keys: infra_error, setup_error, segments{startup,setup,replay,shutdown}, server_exit
    {returncode,signal,expected,phase}|None, timed_out, session_errors[str], invalid_sql_error."""
    segs = ev.get("segments", {})
    kind = oracle["kind"]
    req, exc = oracle.get("required", []), oracle.get("excluded", [])

    if ev.get("infra_error"):
        v = Verdict(INVALID, ev["infra_error"], infra=True)
        if req and kind in ("crash", "pattern"):
            for phase in ("startup", "setup"):
                ok, excerpts, _ = match_all(segs.get(phase, ""), req, exc)
                if ok:
                    v.detail += f" [target signature observed during {phase} phase]"
                    v.excerpts = excerpts
                    v.other_failure = f"target-during-{phase}"
        return v
    if ev.get("invalid_sql_error"):
        return Verdict(INVALID, f"disallowed SQL error: {ev['invalid_sql_error']}", infra=False)

    text_log = segs.get("replay", "")
    if oracle.get("phase") == "replay+shutdown":
        text_log += segs.get("shutdown", "")
    sx = ev.get("server_exit")
    unexpected_exit = bool(sx and not sx.get("expected"))
    other = None
    if unexpected_exit:
        other = (f"server exited unexpectedly in {sx.get('phase')} phase "
                 f"(rc={sx.get('returncode')}, signal={sx.get('signal')})")
        allowed = {"replay"} | ({"shutdown"} if oracle.get("phase") == "replay+shutdown" else set())
        if sx.get("phase") not in allowed:
            return Verdict(NO_TARGET, "termination outside the oracle phase", other_failure=other)

    if kind == "crash":
        if unexpected_exit:
            ok, excerpts, excl = match_all(text_log, req, exc)
            if ok:
                return Verdict(TARGET, other, excerpts=excerpts)
            why = f"excluded pattern {excl!r} matched" if excl else "crash signature did not match"
            return Verdict(NO_TARGET, why, other_failure=other or "crash")
        if ev.get("timed_out"):
            return Verdict(UNRESOLVED, "replay exceeded its time budget without a crash")
        return Verdict(NO_TARGET, "no unexpected server termination")

    if kind == "pattern":
        parts = []
        if "error_log" in oracle.get("sources", ["error_log"]):
            parts.append(text_log)
        if "session_errors" in oracle.get("sources", []):
            parts.append("\n".join(ev.get("session_errors", [])))
        ok, excerpts, excl = match_all("\n".join(parts), req, exc)
        if ok:
            return Verdict(TARGET, "required patterns matched", excerpts=excerpts, other_failure=other)
        if unexpected_exit:
            return Verdict(NO_TARGET, "server terminated without the target pattern", other_failure=other)
        if ev.get("timed_out"):
            return Verdict(UNRESOLVED, "replay exceeded its time budget")
        return Verdict(NO_TARGET, f"excluded pattern {excl!r} matched" if excl else "pattern not found")

    if kind == "sql_probe":
        if unexpected_exit:
            return Verdict(NO_TARGET, "server terminated; probe impossible", other_failure=other)
        if ev.get("timed_out"):
            return Verdict(UNRESOLVED, "replay exceeded its time budget")
        status, text = run_probe() if run_probe else ("error", "no probe runner")
        if status != "ok":
            return Verdict(NO_TARGET, f"probe failed: {text}")
        if re.search(oracle["probe"]["expect_regex"], text, re.MULTILINE):
            return Verdict(TARGET, "probe result matched", excerpts=[{"pattern": oracle["probe"]["expect_regex"],
                                                                     "match": text[:500], "context": text[:4000]}])
        return Verdict(NO_TARGET, f"probe result did not match: {text[:200]!r}")

    if kind == "checker":
        ch = oracle["checker"]
        assert attempt_dir is not None
        ev_path = attempt_dir / "evidence.json"
        ev_path.write_text(json.dumps(ev, indent=1, default=str))
        env = dict(os.environ)
        env.update({k: str(v) for k, v in (checker_env or {}).items()})
        env["PRD_EVIDENCE"] = str(ev_path)
        env["PRD_ATTEMPT_DIR"] = str(attempt_dir)
        out_path = attempt_dir / "checker.out"
        with open(out_path, "wb") as out:
            try:
                p = subprocess.Popen(ch["argv"], stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     env=env, start_new_session=True)
            except OSError as e:
                return Verdict(INVALID, f"cannot run checker: {e}", infra=True)
            REGISTRY.add(p, "checker")
            try:
                rc = p.wait(timeout=ch.get("timeout", 60))
            except subprocess.TimeoutExpired:
                kill_group(p, "checker")
                return Verdict(UNRESOLVED, "checker timed out")
            finally:
                REGISTRY.discard(p)
        tail = out_path.read_text(errors="replace")[-1000:]
        if rc in ch["target_codes"]:
            return Verdict(TARGET, f"checker rc={rc}", excerpts=[{"pattern": "checker", "match": tail[:500],
                                                                  "context": tail}], other_failure=other)
        if rc in ch["no_target_codes"]:
            return Verdict(NO_TARGET, f"checker rc={rc}", other_failure=other)
        if rc in ch.get("invalid_codes", []):
            return Verdict(INVALID, f"checker reported invalid (rc={rc})")
        return Verdict(INVALID, f"checker exit code {rc} is not mapped in oracle.checker", infra=True)

    return Verdict(INVALID, f"unknown oracle kind {kind}", infra=True)
