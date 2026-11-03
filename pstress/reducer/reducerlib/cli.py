"""Command-line interface for pstress_reducer."""

from __future__ import annotations

import argparse
import sys
import traceback

from . import commands as C
from .util import EXIT_INTERNAL, TOOL_VERSION, ReducerError


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pstress_reducer.py",
        description="Reduce a captured failing pstress workload to a smaller reproducer of the same failure, "
                    "keeping independent concurrent sessions.")
    p.add_argument("--version", action="version", version=f"%(prog)s {TOOL_VERSION}")
    sub = p.add_subparsers(dest="command", required=True)

    # import ---------------------------------------------------------------------
    i = sub.add_parser("import", help="raw pstress logs (+ explicit step/setup) -> normalized workload manifest")
    src = i.add_mutually_exclusive_group(required=True)
    src.add_argument("--incident-dir", help="directory containing <node>_step_<N>_thread-<K>.sql logs (searched recursively)")
    src.add_argument("--sql-file", action="append", help="plain SQL file, one statement per line; one session per file "
                     "(repeatable)")
    src.add_argument("--manifest", help="validate and adopt a user-written normalized manifest")
    i.add_argument("--step", type=int, help="step to import (required when the incident has several)")
    i.add_argument("--node", help="node name to import (multi-node incidents are not supported for replay)")
    i.add_argument("--setup", help="initial state: none | sql:<dump.sql> | snapshot:<offline datadir copy>")
    i.add_argument("--setup-loader", choices=["mysql-cli", "driver"], default="mysql-cli",
                   help="how setup SQL is loaded (mysql-cli handles DELIMITER)")
    i.add_argument("--snapshot-checksum", choices=["content", "metadata"], default="content")
    i.add_argument("--external-file", action="append", help="file a snapshot depends on (e.g. keyring), recorded with checksum")
    i.add_argument("--database", default="test", help="session default database when no leading USE is logged")
    i.add_argument("--create-database", action="append",
                   help="database created (IF NOT EXISTS) before replay; default: the sessions' databases")
    i.add_argument("--autocommit", choices=["server-default", "on", "off"], default="server-default",
                   help="session autocommit at connect (pstress/libmysqlclient keep the server default)")
    i.add_argument("--exclude-failed", action="store_true", help="drop F (failed) records (not recommended)")
    i.add_argument("--include-incomplete", action="store_true", help="import incomplete final records")
    i.add_argument("--skip-unframed", action="store_true", help="explicitly skip records that cannot be framed")
    i.add_argument("--allow-empty-setup", action="store_true",
                   help="accept an empty initial state for step > 1 (not equivalent to the original run)")
    i.add_argument("--pin-use", action="store_true", help="pin a leading USE statement in every session")
    i.add_argument("--duration-unit", choices=["us", "ms", "unknown"],
                   help="unit of the timing prefix numbers (default: from the pstress revision)")
    i.add_argument("--pstress-revision", help="pstress revision that wrote the logs (default: detected from the ddl log)")
    i.add_argument("--out", required=True, help="output directory for manifest.json and the import report")
    i.add_argument("--force", action="store_true")
    i.set_defaults(func=C.cmd_import)

    def common(sp, workload=True):
        if workload:
            sp.add_argument("--workload", required=True, help="manifest.json, or a directory containing one (import dir/bundle)")
        sp.add_argument("--config", help="JSON run configuration (see examples/config.example.json)")
        sp.add_argument("--workdir", help="work directory (overrides the config)")

    v = sub.add_parser("verify", help="repeated replay of the workload (or a candidate) without reduction")
    common(v)
    v.add_argument("--attempts", type=int, help="valid attempts (default policy.baseline_attempts)")
    v.add_argument("--candidate", help="candidate hash from a reduction work directory")
    v.set_defaults(func=C.cmd_verify)

    r = sub.add_parser("replay", help="replay a workload or exported bundle against fresh managed instances")
    common(r)
    r.add_argument("--attempts", type=int, default=1)
    r.add_argument("--candidate", help="candidate hash from a reduction work directory")
    r.set_defaults(func=C.cmd_replay)

    d = sub.add_parser("reduce", help="verify the baseline, then reduce sessions and statements")
    common(d)
    d.add_argument("--export", help="bundle output directory (default <workdir>/bundle)")
    d.add_argument("--include-snapshot", action="store_true", help="copy a snapshot setup into the bundle")
    d.set_defaults(func=C.cmd_reduce)

    s = sub.add_parser("resume", help="continue a saved reduction after checking inputs and environment")
    s.add_argument("--workdir", required=True)
    s.add_argument("--config", help="new configuration (budgets may change; environment changes need --revalidate)")
    s.add_argument("--revalidate", action="store_true", help="re-verify the best candidate when the environment changed")
    s.add_argument("--extend", action="store_true", help="continue a finished reduction (e.g. with a larger budget)")
    s.add_argument("--export", help="bundle output directory (default <workdir>/bundle)")
    s.add_argument("--include-snapshot", action="store_true")
    s.set_defaults(func=C.cmd_resume)

    e = sub.add_parser("export", help="export the best (or a given) candidate of a reduction as a bundle")
    e.add_argument("--workdir", required=True)
    e.add_argument("--out", required=True)
    e.add_argument("--candidate", help="candidate hash (default: current best)")
    e.add_argument("--include-snapshot", action="store_true")
    e.set_defaults(func=C.cmd_export)

    st = sub.add_parser("status", help="show the state of a reduction work directory")
    st.add_argument("--workdir", required=True)
    st.set_defaults(func=C.cmd_status)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except ReducerError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return e.exit_code
    except KeyboardInterrupt:
        from .lifecycle import REGISTRY
        REGISTRY.kill_all()
        print("Interrupted.", file=sys.stderr)
        return 130
    except Exception:  # noqa: BLE001
        from .lifecycle import REGISTRY
        REGISTRY.kill_all()
        traceback.print_exc()
        return EXIT_INTERNAL
