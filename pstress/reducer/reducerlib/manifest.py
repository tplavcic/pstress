"""Normalized workload manifest (the source of truth) and reduction candidates.

Manifest format version 1 (JSON object):

{
  "format": "pstress-reducer-workload",
  "format_version": 1,
  "created_by": "pstress_reducer 0.1.0",
  "source": {...import provenance: kind, files+sha256, node, step, revision...},
  "capture": {"complete": bool, "limitations": [str, ...]},
  "setup": {
      "kind": "none" | "sql" | "snapshot",
      "path": "<file or directory>",            # sql/snapshot only
      "sha256": "<checksum>",                   # file hash, or tree hash for snapshot
      "checksum_mode": "content" | "metadata",  # snapshot only
      "loader": "mysql-cli" | "driver",         # sql only
      "create_databases": ["test"],             # created (IF NOT EXISTS) before replay
      "external_files": [...],                  # e.g. keyring files a snapshot needs
      "notes": str
  },
  "session_defaults": {"autocommit": "server-default", "charset": "utf8mb4"},
  "sessions": [
     {"id": "default.node.tld:step2:thread-0", "node": ..., "step": 2, "thread": 0,
      "database": "test", "autocommit": "server-default"|true|false, "charset": "utf8mb4",
      "pinned": false,
      "statements": [
         {"id": "t0.000001", "sql": "<exact SQL>", "pinned": false,
          "source": {"file": "...", "line_start": 3, "line_end": 3},
          "orig": {"status": "S"|"F"|"LOST", "rows": 0, "error": "...", "server_gone": false,
                   "timing": {"wallclock": "...", "start_raw": 123, "duration_raw": 456,
                              "logged_unit": "ms", "actual_unit": "us"}}}
      ]}
  ],
  "groups": [["t0.000005", "t1.000010"]],   # inseparable statement groups (may span sessions)
  "eof_policy": "hold-until-all-finished"
}

A *candidate* is the retained subset: {session_id: [statement ids in original order]}.
Sessions absent from the candidate are removed.  Order inside a session is always the
original order, so a candidate is fully described by sets of ids.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Iterable

from .util import ReducerError, canonical_json, sha256_obj, sha256_text

FORMAT = "pstress-reducer-workload"
FORMAT_VERSION = 1
EOF_POLICIES = ("hold-until-all-finished", "close-when-finished")
AUTOCOMMIT_VALUES = ("server-default", True, False)


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ReducerError(f"invalid workload manifest: {msg}")


def validate_manifest(m: dict) -> None:
    _require(isinstance(m, dict), "top level must be an object")
    _require(m.get("format") == FORMAT, f"format must be {FORMAT!r}")
    _require(m.get("format_version") == FORMAT_VERSION,
             f"unsupported format_version {m.get('format_version')!r} (supported: {FORMAT_VERSION})")
    setup = m.get("setup")
    _require(isinstance(setup, dict), "setup must be an object (explicit setup is required)")
    _require(setup.get("kind") in ("none", "sql", "snapshot"), "setup.kind must be none|sql|snapshot")
    if setup["kind"] in ("sql", "snapshot"):
        _require(isinstance(setup.get("path"), str) and setup["path"], "setup.path required")
    if setup["kind"] == "sql":
        _require(setup.get("loader", "mysql-cli") in ("mysql-cli", "driver"), "setup.loader must be mysql-cli|driver")
    _require(m.get("eof_policy", EOF_POLICIES[0]) in EOF_POLICIES, f"eof_policy must be one of {EOF_POLICIES}")
    sessions = m.get("sessions")
    _require(isinstance(sessions, list), "sessions must be a list")
    seen_sessions: set[str] = set()
    seen_stmts: set[str] = set()
    for s in sessions:
        _require(isinstance(s, dict) and isinstance(s.get("id"), str) and s["id"], "each session needs a string id")
        _require(s["id"] not in seen_sessions, f"duplicate session id {s['id']}")
        seen_sessions.add(s["id"])
        _require(s.get("autocommit", "server-default") in AUTOCOMMIT_VALUES,
                 f"session {s['id']}: autocommit must be 'server-default', true or false")
        stmts = s.get("statements")
        _require(isinstance(stmts, list), f"session {s['id']}: statements must be a list")
        for st in stmts:
            _require(isinstance(st, dict) and isinstance(st.get("id"), str) and st["id"],
                     f"session {s['id']}: statement without id")
            _require(st["id"] not in seen_stmts, f"duplicate statement id {st['id']}")
            seen_stmts.add(st["id"])
            _require(isinstance(st.get("sql"), str), f"statement {st['id']}: sql must be a string")
    for g in m.get("groups", []) or []:
        _require(isinstance(g, list) and all(isinstance(x, str) for x in g), "groups must be lists of statement ids")
        for x in g:
            _require(x in seen_stmts, f"group references unknown statement {x}")


class Workload:
    """Read-only view over a validated manifest with fast lookups."""

    def __init__(self, manifest: dict):
        validate_manifest(manifest)
        self.m = manifest
        self.sessions: list[dict] = manifest["sessions"]
        self.session_by_id = {s["id"]: s for s in self.sessions}
        self.stmt_by_id: dict[str, dict] = {}
        self.stmt_session: dict[str, str] = {}
        self.stmt_index: dict[str, int] = {}
        for s in self.sessions:
            for i, st in enumerate(s["statements"]):
                self.stmt_by_id[st["id"]] = st
                self.stmt_session[st["id"]] = s["id"]
                self.stmt_index[st["id"]] = i
        # inseparable groups -> closure map
        self.group_of: dict[str, frozenset[str]] = {}
        for g in manifest.get("groups", []) or []:
            members = set(g)
            for x in list(members):
                if x in self.group_of:
                    members |= self.group_of[x]
            fz = frozenset(members)
            for x in fz:
                self.group_of[x] = fz

    @property
    def hash(self) -> str:
        return sha256_obj(self.m)

    def full_candidate(self) -> "Candidate":
        return Candidate({s["id"]: tuple(st["id"] for st in s["statements"]) for s in self.sessions})

    def pinned_statements(self) -> set[str]:
        """Pinned statements, plus every statement of a pinned session (kept verbatim)."""
        out = set()
        for s in self.sessions:
            for st in s["statements"]:
                if st.get("pinned") or s.get("pinned"):
                    out.add(st["id"])
        return out

    def pinned_sessions(self) -> set[str]:
        return {s["id"] for s in self.sessions if s.get("pinned")}

    def expand_removal(self, ids: Iterable[str]) -> set[str]:
        """Close a removal set over inseparable groups."""
        out: set[str] = set()
        for x in ids:
            out |= self.group_of.get(x, frozenset((x,)))
        return out

    def sql_bytes(self, cand: "Candidate") -> int:
        return sum(len(self.stmt_by_id[x]["sql"].encode("utf-8", "surrogateescape"))
                   for ids in cand.sessions.values() for x in ids)

    def cost(self, cand: "Candidate") -> tuple[int, int, int]:
        """Lexicographic cost: (active sessions, retained non-pinned statements, SQL bytes)."""
        pinned = self.pinned_statements()
        active = sum(1 for ids in cand.sessions.values() if ids)
        stmts = sum(1 for ids in cand.sessions.values() for x in ids if x not in pinned)
        return (active, stmts, self.sql_bytes(cand))

    def pinned_cost(self) -> dict:
        pinned = self.pinned_statements()
        return {"pinned_statements": len(pinned),
                "pinned_sessions": len(self.pinned_sessions()),
                "setup_kind": self.m["setup"]["kind"]}

    def subset_manifest(self, cand: "Candidate") -> dict:
        """A new manifest containing only the candidate's sessions/statements (ids preserved)."""
        m = copy.deepcopy(self.m)
        keep_sessions = []
        for s in m["sessions"]:
            ids = cand.sessions.get(s["id"])
            if not ids:
                continue
            keep = set(ids)
            s["statements"] = [st for st in s["statements"] if st["id"] in keep]
            keep_sessions.append(s)
        m["sessions"] = keep_sessions
        retained = {x for ids in cand.sessions.values() for x in ids}
        m["groups"] = [g for g in (m.get("groups") or []) if all(x in retained for x in g)]
        m.setdefault("derived_from", {})
        m["derived_from"] = {"workload_sha256": self.hash, "candidate_sha256": cand.hash}
        return m

    def session_sql_text(self, session_id: str, ids: Iterable[str] | None = None) -> str:
        """Human-readable SQL for one session (inspection only; replay uses the manifest)."""
        s = self.session_by_id[session_id]
        keep = set(ids) if ids is not None else None
        lines = [f"-- session {session_id} (database={s.get('database')!r}, "
                 f"autocommit={s.get('autocommit', 'server-default')!r})",
                 "-- For inspection only: exact statement boundaries live in manifest.json.",
                 "-- Statements containing ';' or newlines are wrapped with DELIMITER for mysql-client use."]
        for st in s["statements"]:
            if keep is not None and st["id"] not in keep:
                continue
            sql = st["sql"]
            lines.append(f"-- [{st['id']}] orig={st.get('orig', {}).get('status', '?')}"
                         + (" pinned" if st.get("pinned") else ""))
            if ";" in sql or "\n" in sql:
                lines.append("DELIMITER $$__PRD__")
                lines.append(sql + "$$__PRD__")
                lines.append("DELIMITER ;")
            else:
                lines.append(sql + ";")
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class Candidate:
    sessions: dict  # session_id -> tuple(statement ids), original order

    def __post_init__(self):
        object.__setattr__(self, "sessions", {k: tuple(v) for k, v in self.sessions.items() if v})

    @property
    def hash(self) -> str:
        return sha256_text(canonical_json({k: list(v) for k, v in sorted(self.sessions.items())}))

    def to_json(self) -> dict:
        return {"sessions": {k: list(v) for k, v in sorted(self.sessions.items())}}

    @staticmethod
    def from_json(obj: dict) -> "Candidate":
        return Candidate({k: tuple(v) for k, v in obj["sessions"].items()})

    def statement_count(self) -> int:
        return sum(len(v) for v in self.sessions.values())

    def is_empty(self) -> bool:
        return self.statement_count() == 0

    def all_ids(self) -> set[str]:
        return {x for v in self.sessions.values() for x in v}

    def without(self, removed: set[str]) -> "Candidate":
        return Candidate({k: tuple(x for x in v if x not in removed) for k, v in self.sessions.items()})

    def without_sessions(self, removed_sessions: set[str]) -> "Candidate":
        return Candidate({k: v for k, v in self.sessions.items() if k not in removed_sessions})

    def validate_against(self, w: Workload) -> None:
        for sid, ids in self.sessions.items():
            if sid not in w.session_by_id:
                raise ReducerError(f"candidate references unknown session {sid}")
            last = -1
            for x in ids:
                if w.stmt_session.get(x) != sid:
                    raise ReducerError(f"candidate statement {x} not in session {sid}")
                idx = w.stmt_index[x]
                if idx <= last:
                    raise ReducerError(f"candidate session {sid} is not in original order at {x}")
                last = idx
