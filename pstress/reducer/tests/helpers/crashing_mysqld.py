#!/usr/bin/env python3
"""Test wrapper around a real mysqld that injects a genuine crash (SIGSEGV to mysqld,
which then writes its usual "got signal 11" report) once every marker in
PRD_CRASH_MARKERS has appeared in the general query log.  Used only by the
integration tests to get a deterministic, multi-session crash trigger."""

import os
import signal
import subprocess
import sys
import time

real = os.environ.get("PRD_REAL_MYSQLD", "mysqld")
args = sys.argv[1:]
if "--version" in args or "--initialize-insecure" in args:
    os.execvp(real, [real] + args)

datadir = next(a.split("=", 1)[1] for a in args if a.startswith("--datadir="))
glog = os.path.join(os.path.dirname(datadir.rstrip("/")), "general.log")
markers = [m for m in os.environ.get("PRD_CRASH_MARKERS", "CRASH_A,CRASH_B").split(",") if m]
child = subprocess.Popen([real] + args + ["--general-log=1", f"--general-log-file={glog}"])


def forward(signum, frame):
    try:
        child.send_signal(signum)
    except ProcessLookupError:
        pass


signal.signal(signal.SIGTERM, forward)
signal.signal(signal.SIGINT, forward)

fired = False
while True:
    rc = child.poll()
    if rc is not None:
        break
    if not fired:
        try:
            with open(glog, "r", errors="replace") as f:
                text = f.read()
            if all(m in text for m in markers):
                fired = True
                os.kill(child.pid, signal.SIGSEGV)
        except FileNotFoundError:
            pass
    time.sleep(0.05)

sys.exit(128 - rc if rc < 0 else rc)  # shell convention; avoids dumping a core of the wrapper
