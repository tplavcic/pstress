"""Run configuration (JSON): server environment, trial limits, oracle and acceptance policy.

Secrets never live here: the password comes from the environment variable named by
server.password_env or from a 0600 credentials file (server.credentials_file).
See examples/config.example.json for a commented example.
"""

from __future__ import annotations

import copy
import os
import re
import stat
from pathlib import Path
from typing import Any

from .util import ReducerError, read_json, register_secret, sha256_obj

DEFAULTS: dict[str, Any] = {
    "workdir": None,  # required (CLI --workdir overrides)
    "server": {
        "basedir": None,
        "mysqld": None,             # default: <basedir>/bin/mysqld or mysqld-debug, else PATH
        "mysql_client": None,       # default: <basedir>/bin/mysql, else PATH
        "no_defaults": True,        # pass --no-defaults so /etc/my.cnf is never read
        "managed_args": True,       # add --skip-networking --mysqlx=OFF --secure-file-priv= etc.
        "extra_args": [],           # user startup options (argv list)
        "template_datadir": None,   # clean initialized datadir; default: created once in workdir
        "initialize_args": [],      # extra args for --initialize-insecure when creating the template
        "user": "root",
        "password_env": "PSTRESS_REDUCER_PASSWORD",
        "credentials_file": None,
        "startup_timeout": 120,
        "shutdown_timeout": 30,
        "cleanup": "kill",          # kill | shutdown  (how the server is stopped after evidence capture)
        "core_file": False,
    },
    "hooks": {
        "prepare": None,            # argv run after the datadir is prepared, before start
        "post_start": None,         # argv run after readiness, before setup
        "timeout": 120,
    },
    "trial": {
        "connect_timeout": 15,
        "query_timeout": 120,       # per socket read/write inside a worker
        "replay_timeout": 600,      # whole replay phase
        "setup_timeout": 1800,
        "probe_timeout": 60,
        "post_crash_wait": 30,      # seconds to wait for a dying server to finish writing its log
        "sql_error_policy": {
            "mode": "continue",     # continue | abort-session
            "invalid_on_codes": [], # server error codes that make the attempt INVALID
            "abort_on_codes": [],   # server error codes that end that session (attempt stays valid)
        },
    },
    "oracle": {
        "kind": "crash",            # crash | pattern | sql_probe | checker
        "required": [],             # regexes that must all match
        "excluded": [],             # regexes that must not match
        "sources": ["error_log"],   # error_log | session_errors   (pattern kind)
        "phase": "replay",          # replay | replay+shutdown
        "probe": None,              # sql_probe: {"query": ..., "database": ..., "expect_regex": ...}
        "checker": None,            # checker: {"argv": [...], "timeout": 60, "target_codes": [0],
                                    #           "no_target_codes": [1], "invalid_codes": [2]}
    },
    "policy": {
        "baseline_attempts": 10,
        "candidate_max_valid": 10,
        "candidate_accept_hits": 2,
        "max_invalid_per_candidate": 3,
        "final_attempts": 30,
        "final_required_hits": 5,
        "final_fallbacks": 2,
        "max_total_attempts": 20000,
        "max_seconds": 7 * 24 * 3600,
        "max_consecutive_infra_failures": 5,
        "probe_empty_workload": True,
        "trim_unexecuted_tail": True,
        "transaction_blocks": True,
        "max_rounds": 20,
        "low_hit_rate_ratio": 0.25,
    },
    "retention": {
        "keep_target_trials": 5,
        "keep_other_trials": 20,
        "keep_datadirs": False,
    },
}


def _merge(base: dict, over: dict, path: str = "") -> dict:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if k.startswith("_"):
            continue  # comments
        if k not in base:
            raise ReducerError(f"unknown configuration key {path + k!r}")
        if isinstance(base[k], dict) and base[k] and isinstance(v, dict):
            out[k] = _merge(base[k], v, path + k + ".")
        else:
            out[k] = v
    return out


def _which(name: str) -> str | None:
    for d in os.environ.get("PATH", "").split(os.pathsep):
        p = Path(d) / name
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return None


def load_config(path: str | None, overrides: dict | None = None) -> dict:
    raw = read_json(path) if path else {}
    cfg = _merge(DEFAULTS, raw)
    if overrides:
        for dotted, value in overrides.items():
            if value is None:
                continue
            node = cfg
            parts = dotted.split(".")
            for p in parts[:-1]:
                node = node[p]
            node[parts[-1]] = value
    finalize(cfg, base_dir=Path(path).resolve().parent if path else Path.cwd())
    return cfg


def finalize(cfg: dict, base_dir: Path) -> None:
    srv = cfg["server"]

    def absolutize(v):
        if v is None:
            return None
        p = Path(os.path.expanduser(v))
        return str(p if p.is_absolute() else (base_dir / p).resolve())

    srv["basedir"] = absolutize(srv["basedir"])
    srv["template_datadir"] = absolutize(srv["template_datadir"])
    srv["credentials_file"] = absolutize(srv["credentials_file"])
    if cfg["workdir"]:
        cfg["workdir"] = absolutize(cfg["workdir"])
    if not srv["mysqld"]:
        cands = []
        if srv["basedir"]:
            cands = [Path(srv["basedir"]) / "bin" / "mysqld", Path(srv["basedir"]) / "bin" / "mysqld-debug",
                     Path(srv["basedir"]) / "sbin" / "mysqld"]
        found = next((str(c) for c in cands if c.is_file() and os.access(c, os.X_OK)), None)
        srv["mysqld"] = found or _which("mysqld")
    else:
        srv["mysqld"] = absolutize(srv["mysqld"])
    if not srv["mysql_client"]:
        c = Path(srv["basedir"]) / "bin" / "mysql" if srv["basedir"] else None
        srv["mysql_client"] = str(c) if c and c.is_file() else _which("mysql")
    else:
        srv["mysql_client"] = absolutize(srv["mysql_client"])
    if not isinstance(srv["extra_args"], list) or not all(isinstance(a, str) for a in srv["extra_args"]):
        raise ReducerError("server.extra_args must be a list of strings (argv), not a shell string")
    if srv["cleanup"] not in ("kill", "shutdown"):
        raise ReducerError("server.cleanup must be kill|shutdown")
    for hk in ("prepare", "post_start"):
        v = cfg["hooks"][hk]
        if v is not None and (not isinstance(v, list) or not all(isinstance(a, str) for a in v)):
            raise ReducerError(f"hooks.{hk} must be an argv list of strings")
    o = cfg["oracle"]
    if o["kind"] not in ("crash", "pattern", "sql_probe", "checker"):
        raise ReducerError("oracle.kind must be crash|pattern|sql_probe|checker")
    if o["phase"] not in ("replay", "replay+shutdown"):
        raise ReducerError("oracle.phase must be replay|replay+shutdown")
    for rx in list(o["required"]) + list(o["excluded"]):
        try:
            re.compile(rx)
        except re.error as e:
            raise ReducerError(f"bad oracle regex {rx!r}: {e}")
    if o["kind"] == "crash" and not o["required"]:
        raise ReducerError("oracle.kind=crash needs at least one required signature regex: an unexpected "
                           "exit alone is not enough to identify a specific bug")
    if o["kind"] == "pattern" and not o["required"]:
        raise ReducerError("oracle.kind=pattern needs at least one required regex")
    if o["kind"] == "sql_probe":
        pr = o.get("probe") or {}
        if not pr.get("query") or not pr.get("expect_regex"):
            raise ReducerError("oracle.kind=sql_probe needs probe.query and probe.expect_regex")
    if o["kind"] == "checker":
        ch = o.get("checker") or {}
        if not isinstance(ch.get("argv"), list) or not ch["argv"]:
            raise ReducerError("oracle.kind=checker needs checker.argv (list)")
        for k in ("target_codes", "no_target_codes"):
            if not isinstance(ch.get(k), list) or not ch[k]:
                raise ReducerError(f"oracle.checker.{k} must be a non-empty list; checker exit codes are "
                                   "mapped explicitly (no implicit 'nonzero means target')")
        ch.setdefault("invalid_codes", [])
        ch.setdefault("timeout", 60)
    for k in ("sources",):
        for s in o[k]:
            if s not in ("error_log", "session_errors"):
                raise ReducerError("oracle.sources entries must be error_log|session_errors")
    p = cfg["policy"]
    for k in ("baseline_attempts", "candidate_max_valid", "candidate_accept_hits", "final_attempts",
              "final_required_hits"):
        if not isinstance(p[k], int) or p[k] < 1:
            raise ReducerError(f"policy.{k} must be a positive integer")
    if p["candidate_accept_hits"] > p["candidate_max_valid"]:
        raise ReducerError("policy.candidate_accept_hits cannot exceed policy.candidate_max_valid")
    if p["final_required_hits"] > p["final_attempts"]:
        raise ReducerError("policy.final_required_hits cannot exceed policy.final_attempts")
    esp = cfg["trial"]["sql_error_policy"]
    if esp["mode"] not in ("continue", "abort-session"):
        raise ReducerError("trial.sql_error_policy.mode must be continue|abort-session")


def require_server(cfg: dict) -> None:
    srv = cfg["server"]
    if not srv["mysqld"] or not os.access(srv["mysqld"], os.X_OK):
        raise ReducerError("mysqld binary not found: set server.basedir or server.mysqld")


def password(cfg: dict) -> str:
    srv = cfg["server"]
    if srv["credentials_file"]:
        p = Path(srv["credentials_file"])
        st = p.stat()
        if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise ReducerError(f"credentials file {p} must not be accessible by group/others (chmod 600)")
        data = read_json(p)
        pw = data.get("password", "")
    else:
        pw = os.environ.get(srv["password_env"] or "", "")
    register_secret(pw)
    return pw


def environment_fingerprint(cfg: dict, server_version: str | None, mysqld_sha256: str | None,
                            template_sha256: str | None) -> dict:
    """Everything about the environment that can change trial outcomes (no secrets)."""
    srv = cfg["server"]
    return {
        "mysqld": srv["mysqld"],
        "mysqld_sha256": mysqld_sha256,
        "server_version": server_version,
        "no_defaults": srv["no_defaults"],
        "managed_args": srv["managed_args"],
        "extra_args": srv["extra_args"],
        "template_sha256": template_sha256,
        "initialize_args": srv["initialize_args"],
        "user": srv["user"],
        "hooks": cfg["hooks"],
        "cleanup": srv["cleanup"],
    }


def scheduling_policy(cfg: dict, manifest: dict) -> dict:
    t = cfg["trial"]
    return {"mode": "free-run-with-start-gate", "eof_policy": manifest.get("eof_policy"),
            "sql_error_policy": t["sql_error_policy"], "query_timeout": t["query_timeout"],
            "replay_timeout": t["replay_timeout"], "seed_sequence": None}


def oracle_hash(cfg: dict) -> str:
    return sha256_obj(cfg["oracle"])
