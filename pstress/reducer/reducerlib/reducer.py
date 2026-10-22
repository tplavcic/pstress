"""Hierarchical delta-debugging over sessions and statements.

The algorithm is independent of how candidates are tested: it calls
test(candidate, round_no, label) -> bool, which applies the replay/oracle acceptance
policy.  This keeps it unit-testable with deterministic and flaky fake oracles.

Passes per round (each driven by the same acceptance policy):
  0. tail-trim       drop statements never sent in the latest target attempt (cheap heuristic)
  1. sessions        ddmin over whole sessions (groups of sessions first)
  2. aligned-chunks  remove the same relative slice from all sessions at once, so paired
                     setup/use fragments in different sessions can disappear together
  3. transactions    ddmin over recognized BEGIN/START TRANSACTION ... COMMIT/ROLLBACK blocks
  4. statements      per-session ddmin down to single statements
Rounds repeat until a full round accepts nothing, or the budget is exhausted.

Only strictly smaller candidates under the lexicographic cost
(active sessions, retained non-pinned statements, SQL bytes) are ever tested.
"""

from __future__ import annotations

import re
from typing import Callable, Iterable

from .manifest import Candidate, Workload

PASSES = ("tail-trim", "sessions", "aligned-chunks", "transactions", "statements")
TXN_BEGIN = re.compile(r"^\s*(START\s+TRANSACTION|BEGIN(\s+WORK)?\s*$|BEGIN\s*;?\s*$|XA\s+START)", re.IGNORECASE)
TXN_END = re.compile(r"^\s*(COMMIT|ROLLBACK(?!\s+TO)|XA\s+ROLLBACK)\b", re.IGNORECASE)


def split_chunks(units: list, n: int) -> list[list]:
    n = max(1, min(n, len(units)))
    size, rem = divmod(len(units), n)
    out, i = [], 0
    for k in range(n):
        j = i + size + (1 if k < rem else 0)
        if j > i:
            out.append(units[i:j])
        i = j
    return out


class Reducer:
    def __init__(self, w: Workload, best: Candidate, test: Callable[[Candidate, int, str], bool], *,
                 state: dict | None = None, on_accept: Callable[[Candidate, str], None] | None = None,
                 on_cursor: Callable[[dict], None] | None = None,
                 tail_hint: Callable[[Candidate], dict | None] | None = None,
                 max_rounds: int = 20, transaction_blocks: bool = True, trim_tail: bool = True):
        self.w = w
        self.best = best
        self._test = test
        self.on_accept = on_accept or (lambda c, label: None)
        self.on_cursor = on_cursor or (lambda st: None)
        self.tail_hint = tail_hint
        self.max_rounds = max_rounds
        self.enabled = {"tail-trim": trim_tail and tail_hint is not None, "sessions": True,
                        "aligned-chunks": True, "transactions": transaction_blocks, "statements": True}
        self.state = state or {}
        self.state.setdefault("round", 1)
        self.state.setdefault("pass", PASSES[0])
        self.state.setdefault("granularity", 2)
        self.state.setdefault("session", None)
        self.state.setdefault("round_improved", False)
        self.state.setdefault("rounds_completed", 0)
        self.state.setdefault("last_round_clean", False)
        self.pinned = w.pinned_statements()
        self.pinned_sessions = w.pinned_sessions()
        self.tested = 0
        self.accepted = 0
        self._resume_n: int | None = None

    # ------------------------------------------------------------------
    def cost(self, c: Candidate):
        return self.w.cost(c)

    def try_candidate(self, cand: Candidate, label: str) -> bool:
        if cand.is_empty():
            return False  # the empty workload is probed separately as a diagnostic
        if not (self.cost(cand) < self.cost(self.best)):
            return False
        self.tested += 1
        if self._test(cand, self.state["round"], label):
            self.best = cand
            self.accepted += 1
            self.state["round_improved"] = True
            self.on_accept(cand, label)
            return True
        return False

    def _cursor(self, **kw) -> None:
        self.state.update(kw)
        self.on_cursor(dict(self.state))

    def removal(self, ids: Iterable[str]) -> set[str] | None:
        """Close over inseparable groups; None if that would remove a pinned statement."""
        rem = self.w.expand_removal(ids)
        if rem & self.pinned:
            return None
        return rem

    # ------------------------------------------------------------------
    def ddmin(self, units: list, make: Callable[[list], Candidate | None], label: str, cursor: dict) -> None:
        n = self._resume_n or 2
        self._resume_n = None
        while units:
            if len(units) == 1:
                n = 1
            self._cursor(granularity=n, **cursor)
            chunks = split_chunks(units, n)
            progressed = False
            for ch in chunks:
                cand = make(ch)
                if cand is None:
                    continue
                if self.try_candidate(cand, f"{label} n={n} size={len(ch)}"):
                    removed = set(map(_key, ch))
                    units = [u for u in units if _key(u) not in removed]
                    n = max(n - 1, 2) if len(units) > 1 else 1
                    progressed = True
                    break
            if progressed:
                continue
            if n >= len(units):
                break
            n = min(n * 2, len(units))

    # ------------------------------------------------------------------
    def pass_tail_trim(self) -> None:
        hint = self.tail_hint(self.best) if self.tail_hint else None
        if not hint:
            return
        keep = {}
        for sid, ids in self.best.sessions.items():
            last = hint.get(sid)
            if last is None or last not in ids:
                if sid in self.pinned_sessions or any(x in self.pinned for x in ids):
                    keep[sid] = ids
                continue
            cut = ids.index(last) + 1
            keep[sid] = ids[:cut] + tuple(x for x in ids[cut:] if x in self.pinned)
        cand = Candidate(keep)
        removed = self.best.all_ids() - cand.all_ids()
        rem = self.removal(removed)
        if rem is None or not removed:
            return
        self.try_candidate(self.best.without(rem), "tail-trim")

    def pass_sessions(self) -> None:
        units = [sid for sid in self.best.sessions
                 if sid not in self.pinned_sessions and not any(x in self.pinned for x in self.best.sessions[sid])]
        if len(self.best.sessions) < 2 or not units:
            return

        def make(ch):
            ids = [x for sid in ch for x in self.best.sessions.get(sid, ())]
            rem = self.removal(ids)
            return None if rem is None else self.best.without(rem)
        self.ddmin(units, make, "sessions", {"session": None})

    def pass_aligned(self) -> None:
        active = [sid for sid, ids in self.best.sessions.items() if ids]
        if len(active) < 2:
            return
        longest = max(len(self.best.sessions[s]) for s in active)
        buckets = min(64, longest)
        if buckets < 2:
            return

        def ids_in(bset):
            out = []
            for sid in active:
                ids = self.best.sessions.get(sid, ())
                L = len(ids)
                for i, x in enumerate(ids):
                    if x in self.pinned:
                        continue
                    if (i * buckets) // L in bset:
                        out.append(x)
            return out

        def make(ch):
            ids = ids_in(set(ch))
            if not ids:
                return None
            rem = self.removal(ids)
            return None if rem is None else self.best.without(rem)
        self.ddmin(list(range(buckets)), make, "aligned-chunks", {"session": None})

    def txn_blocks(self, ids: tuple) -> list[tuple]:
        blocks, cur = [], None
        for x in ids:
            sql = self.w.stmt_by_id[x]["sql"]
            if cur is None:
                if TXN_BEGIN.match(sql):
                    cur = [x]
            else:
                cur.append(x)
                if TXN_END.match(sql):
                    blocks.append(tuple(cur))
                    cur = None
                elif TXN_BEGIN.match(sql):  # implicit commit by a new BEGIN: restart block
                    cur = [x]
        return [b for b in blocks if len(b) > 1]

    def _sessions_by_size(self, start_at: str | None) -> list[str]:
        order = sorted(self.best.sessions, key=lambda s: (-len(self.best.sessions[s]), s))
        if start_at and start_at in order:
            order = order[order.index(start_at):]
        return order

    def pass_transactions(self, start_at: str | None) -> None:
        for sid in self._sessions_by_size(start_at):
            ids = self.best.sessions.get(sid)
            if not ids:
                continue
            blocks = self.txn_blocks(ids)
            if not blocks:
                continue

            def make(ch, sid=sid):
                rem = self.removal([x for b in ch for x in b])
                return None if rem is None else self.best.without(rem)
            self.ddmin(blocks, make, f"transactions[{sid}]", {"session": sid})

    def pass_statements(self, start_at: str | None) -> None:
        for sid in self._sessions_by_size(start_at):
            ids = [x for x in self.best.sessions.get(sid, ()) if x not in self.pinned]
            if not ids:
                continue

            def make(ch):
                rem = self.removal(ch)
                return None if rem is None else self.best.without(rem)
            self.ddmin(ids, make, f"statements[{sid}]", {"session": sid})

    # ------------------------------------------------------------------
    def run(self) -> Candidate:
        """Run rounds from the saved cursor.  BudgetExhausted/KeyboardInterrupt propagate."""
        resume_pass = self.state["pass"]
        resume_session = self.state.get("session")
        self._resume_n = self.state.get("granularity") or None
        while self.state["round"] <= self.max_rounds:
            start = PASSES.index(resume_pass) if resume_pass in PASSES else 0
            for p in PASSES[start:]:
                sess = resume_session if p == resume_pass else None
                if p != resume_pass:
                    self._resume_n = None
                self._cursor(**{"pass": p, "session": sess})
                if not self.enabled[p]:
                    continue
                if p == "tail-trim":
                    self.pass_tail_trim()
                elif p == "sessions":
                    self.pass_sessions()
                elif p == "aligned-chunks":
                    self.pass_aligned()
                elif p == "transactions":
                    self.pass_transactions(sess)
                elif p == "statements":
                    self.pass_statements(sess)
            resume_pass, resume_session = PASSES[0], None
            improved = self.state["round_improved"]
            self.state["rounds_completed"] = self.state["round"]
            self.state["last_round_clean"] = not improved
            self._cursor(round=self.state["round"] + 1, **{"pass": PASSES[0]}, session=None, granularity=2,
                         round_improved=False)
            if not improved:
                break
        return self.best


def _key(u):
    return u if isinstance(u, (str, int)) else tuple(u)
