"""Milestone C: reduction algorithm, acceptance policy, persistence, resume and export.

Uses injected deterministic test oracles instead of a database: trial.run_attempt is
replaced by a fake that decides the outcome from the candidate content.
"""

import random
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from reducerlib import commands, trial  # noqa: E402
from reducerlib.config import DEFAULTS, _merge  # noqa: E402
from reducerlib.manifest import Candidate, Workload  # noqa: E402
from reducerlib.reducer import Reducer, split_chunks  # noqa: E402
from reducerlib.util import read_json  # noqa: E402


def make_workload(n_sessions=4, n_stmts=12, extra=None, groups=None, pinned=()):
    sessions = []
    for s in range(n_sessions):
        stmts = [{"id": f"s{s}.{i:03d}", "sql": f"SELECT {s * 100 + i}", "pinned": f"s{s}.{i:03d}" in pinned}
                 for i in range(n_stmts)]
        sessions.append({"id": f"sess{s}", "database": "test", "statements": stmts})
    if extra:
        extra(sessions)
    m = {"format": "pstress-reducer-workload", "format_version": 1, "setup": {"kind": "none"},
         "sessions": sessions, "groups": groups or [], "eof_policy": "hold-until-all-finished"}
    return Workload(m)


def contains(c: Candidate, *ids):
    allids = c.all_ids()
    return all(i in allids for i in ids)


class AlgorithmTests(unittest.TestCase):
    def run_reducer(self, w, pred, **kw):
        calls = []

        def test(c, r, label):
            calls.append(label)
            return pred(c)
        red = Reducer(w, w.full_candidate(), test, **kw)
        return red.run(), red, calls

    def test_split_chunks(self):
        self.assertEqual(split_chunks(list(range(5)), 2), [[0, 1, 2], [3, 4]])
        self.assertEqual(split_chunks([1], 4), [[1]])

    def test_two_required_sessions_survive_noise_removed(self):
        w = make_workload()
        best, red, _ = self.run_reducer(w, lambda c: contains(c, "s1.005", "s2.009"))
        self.assertEqual(best.sessions, {"sess1": ("s1.005",), "sess2": ("s2.009",)})
        self.assertTrue(red.state["last_round_clean"])

    def test_whole_sessions_removed_first(self):
        w = make_workload(n_sessions=6, n_stmts=30)
        _, _, calls = self.run_reducer(w, lambda c: contains(c, "s3.001", "s4.029"))
        sess = [i for i, lbl in enumerate(calls) if lbl.startswith("sessions")]
        other = [i for i, lbl in enumerate(calls) if not lbl.startswith("sessions")]
        self.assertEqual(sess[0], 0)
        self.assertGreaterEqual(min(other), 3)  # round 1 starts with several whole-session trials

    def test_ordering_constraint_inside_session_preserved(self):
        w = make_workload(n_sessions=2, n_stmts=20)

        def pred(c):
            ids = c.sessions.get("sess0", ())
            return "s0.003" in ids and "s0.017" in ids and ids.index("s0.003") < ids.index("s0.017")
        best, _, _ = self.run_reducer(w, pred)
        self.assertEqual(best.sessions, {"sess0": ("s0.003", "s0.017")})

    def test_pinned_statements_and_sessions_never_removed(self):
        w = make_workload(pinned=("s0.000",))
        w.m["sessions"][3]["pinned"] = True
        w = Workload(w.m)
        best, _, _ = self.run_reducer(w, lambda c: contains(c, "s1.004"))
        self.assertIn("s0.000", best.all_ids())
        self.assertIn("sess3", best.sessions)
        self.assertEqual(set(best.all_ids()) - {"s0.000"} - set(best.sessions["sess3"]), {"s1.004"})

    def test_groups_removed_together(self):
        w = make_workload(groups=[["s0.002", "s1.007"]])
        seen = []

        def pred(c):
            ids = c.all_ids()
            seen.append(("s0.002" in ids) == ("s1.007" in ids))
            return contains(c, "s2.001")
        best, _, _ = self.run_reducer(w, pred)
        self.assertTrue(all(seen), "a candidate separated an inseparable group")
        self.assertEqual(best.all_ids(), {"s2.001"})

    def test_non_monotonic_dependency(self):
        # target needs A and B, but C hides it unless D is also present
        w = make_workload(n_sessions=2, n_stmts=16)
        A, B, C, D = "s0.002", "s1.010", "s0.008", "s1.004"

        def pred(c):
            ids = c.all_ids()
            return A in ids and B in ids and (C not in ids or D in ids)
        best, _, _ = self.run_reducer(w, pred)
        self.assertTrue(pred(best))
        for x in best.all_ids():  # 1-minimal with respect to single removals
            self.assertFalse(pred(best.without({x})))

    def test_cross_session_paired_fragments(self):
        # either both halves (s0.000 & s1.000) present, or neither, plus the trigger
        w = make_workload(n_sessions=2, n_stmts=8)

        def pred(c):
            ids = c.all_ids()
            paired_ok = ("s0.000" in ids) == ("s1.000" in ids)
            return paired_ok and "s1.007" in ids
        best, _, calls = self.run_reducer(w, pred)
        self.assertEqual(best.all_ids(), {"s1.007"})
        self.assertTrue(any(lbl.startswith("aligned-chunks") for lbl in calls))

    def test_transaction_blocks(self):
        def extra(sessions):
            sql = ["BEGIN", "UPDATE t SET a=1", "UPDATE t SET a=2", "COMMIT", "SELECT 1", "START TRANSACTION",
                   "DELETE FROM t", "ROLLBACK", "SELECT 2"]
            sessions[0]["statements"] = [{"id": f"x{i}", "sql": s} for i, s in enumerate(sql)]
        w = make_workload(n_sessions=1, extra=extra)
        red = Reducer(w, w.full_candidate(), lambda c, r, label: True)
        blocks = red.txn_blocks(w.full_candidate().sessions["sess0"])
        self.assertEqual(blocks, [("x0", "x1", "x2", "x3"), ("x5", "x6", "x7")])

    def test_tail_trim(self):
        w = make_workload(n_sessions=2, n_stmts=10)
        hint = {"sess0": "s0.004", "sess1": None}
        calls = []

        def test(c, r, label):
            calls.append(label)
            return contains(c, "s0.004")
        red = Reducer(w, w.full_candidate(), test, tail_hint=lambda c: hint)
        red.pass_tail_trim()
        self.assertEqual(calls, ["tail-trim"])
        self.assertEqual(red.best.sessions, {"sess0": tuple(f"s0.{i:03d}" for i in range(5))})

    def test_only_strictly_smaller_candidates_tested(self):
        w = make_workload(n_sessions=2, n_stmts=6)
        sizes = []

        def test(c, r, label):
            sizes.append(w.cost(c))
            return contains(c, "s0.001")
        red = Reducer(w, w.full_candidate(), test)
        red.run()
        self.assertTrue(all(s < w.cost(w.full_candidate()) for s in sizes))
        self.assertNotIn((0, 0, 0), sizes)  # the empty workload is a separate diagnostic


# ---------------------------------------------------------------------------
# Policy / persistence tests with a fake attempt runner
# ---------------------------------------------------------------------------
class FakeAttempts:
    """Deterministic fake of trial.run_attempt.  prob(candidate) -> hit probability."""

    def __init__(self, prob, seed=1, interrupt_at=None, infra=False):
        self.prob = prob
        self.rng = random.Random(seed)
        self.interrupt_at = interrupt_at
        self.infra = infra
        self.calls = 0

    def __call__(self, ctx, cand, purpose):
        self.calls += 1
        ctx.attempts_total += 1
        if self.interrupt_at is not None and ctx.attempts_total == self.interrupt_at:
            raise KeyboardInterrupt()
        if self.infra:
            return {"attempt": ctx.attempts_total, "outcome": "INVALID", "infra": True, "detail": "startup failed",
                    "candidate": cand.hash, "last_sent": {}}
        hit = self.rng.random() < self.prob(cand)
        last = {sid: ids[-1] for sid, ids in cand.sessions.items()}
        return {"attempt": ctx.attempts_total, "outcome": "TARGET" if hit else "NO_TARGET", "infra": False,
                "detail": "", "candidate": cand.hash, "last_sent": last, "other_failure": None}


def make_cfg(**policy):
    cfg = _merge(DEFAULTS, {"policy": policy})
    return cfg


class DriveTests(unittest.TestCase):
    def setUp(self):
        self.d = Path(tempfile.mkdtemp())
        self.orig = trial.run_attempt
        self.w = make_workload(n_sessions=3, n_stmts=8)

    def tearDown(self):
        trial.run_attempt = self.orig
        shutil.rmtree(self.d)

    def ctx(self, cfg, env="env-a"):
        ctx = trial.RunContext(workload=self.w, cfg=cfg, password="", workdir=self.d, version="fake")
        ctx.env_fp = {"fake": env}
        return ctx

    def new_state(self, ctx):
        full = self.w.full_candidate()
        trial.save_candidate(self.d, full)
        return {"state_version": 1, "status": "baseline", "workload_sha256": self.w.hash, "base_key": ctx.base_key(),
                "base_key_hash": ctx.base_key_hash(), "attempts_total": 0, "seconds_used": 0.0, "baseline": None,
                "empty_probe": None, "best": full.hash, "history": [], "tail_hints": {}, "reducer": {},
                "final": [], "budget_exhausted": None}

    def args(self):
        return types.SimpleNamespace(export=str(self.d / "bundle"), include_snapshot=False)

    def test_deterministic_reduction_end_to_end_with_export(self):
        trial.run_attempt = FakeAttempts(lambda c: 1.0 if contains(c, "s0.002", "s2.006") else 0.0)
        cfg = make_cfg(baseline_attempts=3, candidate_max_valid=2, candidate_accept_hits=1, final_attempts=5,
                       final_required_hits=3)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        code = commands._drive(ctx, state, self.args())
        self.assertEqual(code, 0)
        best = trial.load_candidate(self.d, state["best"])
        self.assertEqual(best.sessions, {"sess0": ("s0.002",), "sess2": ("s2.006",)})
        self.assertEqual(state["status"], "verified")
        b = self.d / "bundle"
        m = read_json(b / "manifest.json")
        self.assertEqual([s["id"] for s in m["sessions"]], ["sess0", "sess2"])
        self.assertTrue((b / "README.md").read_text().count("concurrently"))
        self.assertIn("1-minimal", (self.d / "report.txt").read_text())

    def test_baseline_unconfirmed_stops(self):
        trial.run_attempt = FakeAttempts(lambda c: 0.0)
        cfg = make_cfg(baseline_attempts=4)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        self.assertEqual(commands._drive(ctx, state, self.args()), 3)
        self.assertEqual(state["baseline"]["target"], 0)
        self.assertEqual(ctx.attempts_total, 4)

    def test_infra_failures_abort(self):
        trial.run_attempt = FakeAttempts(lambda c: 1.0, infra=True)
        cfg = make_cfg(max_consecutive_infra_failures=3)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        self.assertEqual(commands._drive(ctx, state, self.args()), 4)
        self.assertEqual(ctx.attempts_total, 3)

    def test_interrupt_then_resume_reaches_same_result(self):
        pred = lambda c: 1.0 if contains(c, "s1.001", "s2.007") else 0.0  # noqa: E731
        cfg = make_cfg(baseline_attempts=2, candidate_max_valid=2, candidate_accept_hits=1, final_attempts=3,
                       final_required_hits=2)
        trial.run_attempt = FakeAttempts(pred, interrupt_at=25)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        self.assertEqual(commands._drive(ctx, state, self.args()), 130)
        saved = read_json(self.d / "state.json")
        best_before = trial.load_candidate(self.d, saved["best"])  # readable after interrupt
        self.assertTrue(pred(best_before))
        # resume from the persisted state in a fresh context
        trial.run_attempt = FakeAttempts(pred)
        ctx2 = self.ctx(cfg)
        ctx2.attempts_total = saved["attempts_total"]
        code = commands._drive(ctx2, saved, self.args())
        self.assertEqual(code, 0)
        best = trial.load_candidate(self.d, saved["best"])
        self.assertEqual(best.sessions, {"sess1": ("s1.001",), "sess2": ("s2.007",)})

    def test_budget_exhaustion_keeps_confirmed_partial(self):
        trial.run_attempt = FakeAttempts(lambda c: 1.0 if contains(c, "s0.000") else 0.0)
        cfg = make_cfg(baseline_attempts=2, candidate_max_valid=2, candidate_accept_hits=1, final_attempts=3,
                       final_required_hits=2, max_total_attempts=12)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        code = commands._drive(ctx, state, self.args())
        self.assertEqual(code, 5)
        self.assertTrue(state["budget_exhausted"])
        self.assertEqual(state["status"], "partial-budget-exhausted")
        self.assertIn("partial", read_json(self.d / "report.json")["claim"])

    def test_failed_final_validation_falls_back(self):
        # tiny candidates "pass" during reduction by luck but are unreliable; larger ones are solid
        def prob(c):
            if not contains(c, "s0.000"):
                return 0.0
            return 1.0 if c.statement_count() > 3 else 0.15
        trial.run_attempt = FakeAttempts(prob, seed=7)
        cfg = make_cfg(baseline_attempts=2, candidate_max_valid=6, candidate_accept_hits=1, final_attempts=10,
                       final_required_hits=6, final_fallbacks=30)
        ctx = self.ctx(cfg)
        state = self.new_state(ctx)
        code = commands._drive(ctx, state, self.args())
        self.assertEqual(code, 6)
        self.assertEqual(state["status"], "fell-back")
        self.assertGreater(trial.load_candidate(self.d, state["best"]).statement_count(), 3)
        self.assertIn("previous confirmed", (self.d / "bundle" / "README.md").read_text())

    def test_flaky_negatives_only_cached_per_round(self):
        runner = trial.Runner(self.ctx(make_cfg(candidate_max_valid=3, candidate_accept_hits=1)))
        trial.run_attempt = FakeAttempts(lambda c: 0.0)
        ev = trial.Evaluator(runner)
        c = self.w.full_candidate().without({"s0.000"})
        ok, _ = ev.evaluate(c, 1, "x")
        self.assertFalse(ok)
        n = runner.ctx.attempts_total
        ok, _ = ev.evaluate(c, 1, "x")  # same round: cached, no attempts
        self.assertEqual(runner.ctx.attempts_total, n)
        trial.run_attempt = FakeAttempts(lambda c: 1.0)
        ok, _ = ev.evaluate(c, 2, "x")  # next round: retried, and now it passes
        self.assertTrue(ok)
        rec = ev.cache[ev.key(c)]
        self.assertEqual(rec["no_target"], 3)
        self.assertEqual(rec["target"], 1)

    def test_cache_key_changes_with_environment_and_oracle(self):
        cfg = make_cfg()
        a = trial.Evaluator(trial.Runner(self.ctx(cfg, env="env-a")))
        b = trial.Evaluator(trial.Runner(self.ctx(cfg, env="env-b")))
        cfg2 = make_cfg()
        cfg2["oracle"]["required"] = ["other"]
        c = trial.Evaluator(trial.Runner(self.ctx(cfg2, env="env-a")))
        cand = self.w.full_candidate()
        self.assertNotEqual(a.key(cand), b.key(cand))
        self.assertNotEqual(a.key(cand), c.key(cand))

    def test_acceptance_stops_early(self):
        trial.run_attempt = fa = FakeAttempts(lambda c: 1.0)
        cfg = make_cfg(candidate_max_valid=10, candidate_accept_hits=2)
        ev = trial.Evaluator(trial.Runner(self.ctx(cfg)))
        ok, st = ev.evaluate(self.w.full_candidate().without({"s0.001"}), 1, "x")
        self.assertTrue(ok)
        self.assertEqual(fa.calls, 2)
        trial.run_attempt = fa = FakeAttempts(lambda c: 0.0)
        ok, st = ev.evaluate(self.w.full_candidate().without({"s0.002"}), 1, "y")
        self.assertFalse(ok)
        self.assertEqual(fa.calls, 9)  # after 9 misses 2 hits are impossible within 10


if __name__ == "__main__":
    unittest.main()
