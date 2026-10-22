"""One replay session = one OS process = one connection, executing its stream in order.

Invoked by the coordinator as:  python -m reducerlib.worker <spec.json>

Protocol (line based):
  stdout -> coordinator:  READY | CONNECT_FAILED <msg> | DONE | ABORTED <reason>
  stdin  <- coordinator:  GO (start gate) ; CLOSE or EOF (end of trial, close connection)

Events are appended to spec["events_file"] as JSON lines with time.monotonic() stamps
(CLOCK_MONOTONIC is system wide on Linux, so stamps from different workers are comparable):
  {"ev":"connected","t":..,"connection_id":..}
  {"ev":"send","id":..,"t":..}
  {"ev":"done","id":..,"t":..,"ok":true,"rows":n}
  {"ev":"done","id":..,"t":..,"ok":false,"errno":n,"msg":"..","class":"server|transport|timeout|driver"}
  {"ev":"eof","t":..}  {"ev":"abort","reason":..,"t":..}  {"ev":"close","t":..}

No transparent reconnect: after a transport/driver failure the session stops.
All result sets are consumed before the next statement.  Transaction SQL is executed as
written; nothing is wrapped or committed implicitly.
"""

from __future__ import annotations

import json
import os
import sys
import time

TRANSPORT_ERRNOS = {2006, 2013, 2055, 2002, 2003, 2001, 2026}


def _emit(line: str) -> None:
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def main(argv: list[str]) -> int:
    spec_path = argv[1]
    with open(spec_path, "r", encoding="utf-8") as f:
        spec = json.load(f)
    password = os.environ.get("PRD_PASSWORD", "")
    events = open(spec["events_file"], "a", encoding="utf-8")

    def ev(obj: dict) -> None:
        obj.setdefault("t", time.monotonic())
        events.write(json.dumps(obj, ensure_ascii=True) + "\n")
        events.flush()

    import pymysql
    from pymysql import cursors, err

    statements = []
    with open(spec["statements_file"], "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                statements.append(json.loads(line))

    ac = spec.get("autocommit", "server-default")
    try:
        conn = pymysql.connect(
            unix_socket=spec["socket"], user=spec["user"], password=password,
            database=spec.get("database") or None,
            autocommit=None if ac == "server-default" else bool(ac),
            charset=spec.get("charset") or "utf8mb4",
            connect_timeout=spec["connect_timeout"],
            read_timeout=spec["query_timeout"], write_timeout=spec["query_timeout"],
            use_unicode=False, client_flag=0, local_infile=False)
    except Exception as e:  # noqa: BLE001
        msg = str(e).replace("\n", " ")
        ev({"ev": "connect_failed", "msg": msg})
        _emit("CONNECT_FAILED " + msg)
        return 3
    try:
        cid = conn.thread_id()
    except Exception:  # noqa: BLE001
        cid = None
    ev({"ev": "connected", "connection_id": cid})
    _emit("READY")

    gate = sys.stdin.readline()
    if gate.strip() != "GO":
        ev({"ev": "abort", "reason": "no start gate"})
        conn.close()
        return 0

    policy = spec.get("sql_error_policy") or {}
    invalid_codes = set(policy.get("invalid_on_codes") or [])
    abort_codes = set(policy.get("abort_on_codes") or [])
    abort_any = policy.get("mode") == "abort-session"
    rc = 0
    aborted = None
    cur = conn.cursor(cursors.SSCursor)
    for st in statements:
        sid = st["id"]
        ev({"ev": "send", "id": sid})
        try:
            cur.execute(st["sql"])  # args=None: no client-side % formatting
            rows = 0
            if cur.description is not None:
                for _ in cur:
                    rows += 1
            else:
                rows = cur.rowcount
            while cur.nextset():
                for _ in cur:
                    pass
            ev({"ev": "done", "id": sid, "ok": True, "rows": rows})
        except err.MySQLError as e:
            errno = e.args[0] if e.args and isinstance(e.args[0], int) else -1
            msg = str(e.args[1]) if len(e.args) > 1 else str(e)
            # Client-side (CR_*) errors are 2000-2999; server errors are <2000 or >=3000.
            if errno in TRANSPORT_ERRNOS or (isinstance(errno, int) and 2000 <= errno < 3000):
                klass = "timeout" if "timed out" in msg else "transport"
            elif errno == -1 or isinstance(e, (err.InterfaceError,)):
                klass = "driver"
            else:
                klass = "server"
            ev({"ev": "done", "id": sid, "ok": False, "errno": errno, "msg": msg, "class": klass})
            if klass != "server":
                aborted = f"{klass} error {errno} at {sid}: {msg}"
                rc = 4
                break
            if errno in invalid_codes:
                aborted = f"invalid-sql-error {errno} at {sid}"
                rc = 6
                break
            if abort_any or errno in abort_codes:
                aborted = f"session aborted by sql_error_policy on {errno} at {sid}"
                break
        except Exception as e:  # noqa: BLE001 - driver-side failure
            msg = f"{type(e).__name__}: {e}".replace("\n", " ")
            ev({"ev": "done", "id": sid, "ok": False, "errno": -1, "msg": msg, "class": "driver"})
            aborted = f"driver error at {sid}: {msg}"
            rc = 5
            break
    if aborted:
        ev({"ev": "abort", "reason": aborted})
        _emit("ABORTED " + aborted)
    else:
        ev({"ev": "eof"})
        _emit("DONE")

    if spec.get("eof_policy", "hold-until-all-finished") == "hold-until-all-finished" and rc in (0,):
        sys.stdin.readline()  # CLOSE or EOF from the coordinator
    try:
        conn.close()
    except Exception:  # noqa: BLE001
        pass
    ev({"ev": "close"})
    events.close()
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv))
