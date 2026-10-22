"""Managed standalone server lifecycle adapter.

prepare fresh trial -> restore state -> start server -> readiness -> (hooks) -> load setup
-> [replay elsewhere] -> collect evidence -> cleanup

Every attempt gets its own datadir, socket, pid file, tmpdir and error log inside the
attempt directory.  The server is started in its own process group and only that group
is ever signalled: no killall, no matching by process name.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .util import ReducerError, copy_tree_no_links, log, redact_argv, safe_rmtree, sha256_file, warn


# ---------------------------------------------------------------------------
# Owned process registry: everything we spawn is tracked so Ctrl+C can stop it.
# ---------------------------------------------------------------------------
class ProcessRegistry:
    def __init__(self):
        self._procs: dict[int, tuple[subprocess.Popen, str]] = {}

    def add(self, proc: subprocess.Popen, label: str) -> None:
        self._procs[proc.pid] = (proc, label)

    def discard(self, proc: subprocess.Popen) -> None:
        self._procs.pop(proc.pid, None)

    def kill_all(self) -> None:
        for pid, (proc, label) in list(self._procs.items()):
            kill_group(proc, label)
        self._procs.clear()


REGISTRY = ProcessRegistry()


def kill_group(proc: subprocess.Popen, label: str = "", sig: int = signal.SIGKILL, wait: float = 10.0) -> None:
    """Signal the process group we created for proc (start_new_session=True) and reap it."""
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            pass
        except PermissionError:
            try:
                proc.send_signal(sig)
            except ProcessLookupError:
                pass
    try:
        proc.wait(timeout=wait)
    except subprocess.TimeoutExpired:
        warn(f"owned process {label} pid={proc.pid} did not exit after signal {sig}")
    # also make sure nothing else in that group survives (e.g. a wrapper's child)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def server_version(mysqld: str) -> str:
    try:
        out = subprocess.run([mysqld, "--no-defaults", "--version"], capture_output=True, text=True, timeout=60)
        return (out.stdout or out.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        raise ReducerError(f"cannot run {mysqld} --version: {e}")


def version_tuple(version_text: str) -> tuple[int, int, int]:
    m = re.search(r"Ver (\d+)\.(\d+)\.(\d+)", version_text)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else (0, 0, 0)


def ensure_template(cfg: dict, workdir: Path) -> Path:
    """Return a clean initialized datadir; create it once in the workdir if not configured."""
    srv = cfg["server"]
    if srv["template_datadir"]:
        p = Path(srv["template_datadir"])
        if not p.is_dir():
            raise ReducerError(f"server.template_datadir {p} does not exist")
        return p
    tdir = workdir / "template"
    data = tdir / "data"
    marker = tdir / "READY"
    if marker.exists():
        return data
    if tdir.exists():
        safe_rmtree(tdir, workdir)
    tdir.mkdir(parents=True)
    argv = [srv["mysqld"]]
    if srv["no_defaults"]:
        argv.append("--no-defaults")
    if srv["basedir"]:
        argv.append(f"--basedir={srv['basedir']}")
    argv += ["--initialize-insecure", f"--datadir={data}", f"--log-error={tdir / 'init.err'}"]
    if os.geteuid() == 0:
        argv.append("--user=root")
    argv += list(srv["initialize_args"])
    log(f"initializing template datadir: {' '.join(redact_argv(argv))}")
    with open(tdir / "init.out", "wb") as out:
        proc = subprocess.Popen(argv, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
        REGISTRY.add(proc, "mysqld --initialize")
        try:
            rc = proc.wait(timeout=600)
        except subprocess.TimeoutExpired:
            kill_group(proc, "mysqld --initialize")
            raise ReducerError("template initialization timed out", 4)
        finally:
            REGISTRY.discard(proc)
    if rc != 0:
        raise ReducerError(f"template initialization failed (rc={rc}); see {tdir / 'init.err'}", 4)
    marker.write_text(time.strftime("%Y-%m-%dT%H:%M:%S") + "\n")
    return data


def _socket_path(attempt_dir: Path) -> Path:
    p = attempt_dir / "mysqld.sock"
    if len(str(p)) < 100:
        return p
    # Unix socket paths are limited to ~107 bytes; use a private short directory instead.
    base = Path("/tmp") / f"pstress-reducer-{os.getuid()}"
    base.mkdir(mode=0o700, exist_ok=True)
    if base.stat().st_uid != os.getuid():
        raise ReducerError(f"{base} is not owned by the current user")
    import hashlib
    return base / (hashlib.sha1(str(attempt_dir).encode()).hexdigest()[:16] + ".sock")


@dataclass
class ServerExit:
    returncode: int | None
    signal: int | None
    expected: bool          # True when the reducer itself stopped/killed the server
    when: float


@dataclass
class ManagedServer:
    cfg: dict
    attempt_dir: Path
    owned_root: Path
    version: str
    proc: subprocess.Popen | None = None
    exit: ServerExit | None = None
    expected_stop: bool = False
    exit_phase: str | None = None
    phase: str = "prepare"
    offsets: dict = field(default_factory=dict)

    def __post_init__(self):
        self.datadir = self.attempt_dir / "data"
        self.tmpdir = self.attempt_dir / "tmp"
        self.error_log = self.attempt_dir / "mysqld.err"
        self.pid_file = self.attempt_dir / "mysqld.pid"
        self.socket = _socket_path(self.attempt_dir)
        self.client_cnf = self.attempt_dir / "client.cnf"

    # -- prepare ------------------------------------------------------------
    def prepare(self, source_datadir: Path) -> None:
        if self.datadir.exists():
            safe_rmtree(self.datadir, self.owned_root)
        copy_tree_no_links(source_datadir, self.datadir)
        self.tmpdir.mkdir(parents=True, exist_ok=True)
        if self.socket.exists():
            self.socket.unlink()

    def argv(self) -> list[str]:
        srv = self.cfg["server"]
        a = [srv["mysqld"]]
        if srv["no_defaults"]:
            a.append("--no-defaults")
        if srv["basedir"]:
            a.append(f"--basedir={srv['basedir']}")
        a += [f"--datadir={self.datadir}", f"--socket={self.socket}", f"--pid-file={self.pid_file}",
              f"--log-error={self.error_log}", f"--tmpdir={self.tmpdir}"]
        if srv["managed_args"]:
            a += ["--skip-networking", "--secure-file-priv="]
            if version_tuple(self.version) >= (8, 0, 0):
                a.append("--mysqlx=OFF")
            if os.geteuid() == 0:
                a.append("--user=root")
        if srv["core_file"]:
            a.append("--core-file")
        a += list(srv["extra_args"])
        return a

    # -- start / readiness ---------------------------------------------------
    def start(self) -> None:
        argv = self.argv()
        (self.attempt_dir / "server-argv.txt").write_text("\n".join(redact_argv(argv)) + "\n")
        self.stdout_f = open(self.attempt_dir / "mysqld.stdout", "wb")
        self.proc = subprocess.Popen(argv, stdout=self.stdout_f, stderr=subprocess.STDOUT,
                                     stdin=subprocess.DEVNULL, start_new_session=True)
        REGISTRY.add(self.proc, "mysqld")

    def poll(self) -> ServerExit | None:
        if self.exit is not None:
            return self.exit
        if self.proc is None:
            return None
        rc = self.proc.poll()
        if rc is None:
            return None
        sig = -rc if rc < 0 else None
        self.exit = ServerExit(returncode=rc, signal=sig, expected=self.expected_stop, when=time.monotonic())
        self.exit_phase = self.phase
        REGISTRY.discard(self.proc)
        return self.exit

    def alive(self) -> bool:
        return self.poll() is None

    def connect(self, password: str, database: str | None = None, timeout: float = 5, read_timeout=None):
        import pymysql
        return pymysql.connect(unix_socket=str(self.socket), user=self.cfg["server"]["user"], password=password,
                               database=database, connect_timeout=timeout, read_timeout=read_timeout,
                               write_timeout=read_timeout, autocommit=True, charset="utf8mb4")

    def wait_ready(self, password: str, timeout: float) -> tuple[bool, str]:
        deadline = time.monotonic() + timeout
        last = ""
        while time.monotonic() < deadline:
            if not self.alive():
                return False, f"mysqld exited during startup (rc={self.exit.returncode})"
            if self.socket.exists():
                try:
                    c = self.connect(password, timeout=3, read_timeout=10)
                    c.close()
                    return True, ""
                except Exception as e:  # noqa: BLE001 - any failure means not ready yet
                    last = str(e)
            time.sleep(0.2)
        return False, f"server not ready after {timeout}s ({last})"

    def log_offset(self) -> int:
        try:
            return self.error_log.stat().st_size
        except FileNotFoundError:
            return 0

    def mark(self, name: str) -> None:
        self.offsets[name] = self.log_offset()

    def write_client_cnf(self, password: str) -> None:
        user = self.cfg["server"]["user"]
        text = f"[client]\nuser={user}\nsocket={self.socket}\n"
        if password:
            text += f"password={password}\n"
        fd = os.open(self.client_cnf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(text)

    # -- stop / cleanup --------------------------------------------------------
    def stop(self, password: str, mode: str, timeout: float) -> None:
        if self.proc is None:
            return
        if self.poll() is not None:
            return
        if mode == "shutdown":
            # A graceful shutdown requested by the reducer: a clean exit (rc 0) is expected,
            # a signal or non-zero exit while shutting down is recorded as an unexpected
            # termination in the "shutdown" phase (only matters for oracle.phase=replay+shutdown).
            try:
                c = self.connect(password, timeout=3, read_timeout=timeout)
                try:
                    c.cursor().execute("SHUTDOWN")
                finally:
                    try:
                        c.close()
                    except Exception:  # noqa: BLE001
                        pass
            except Exception:  # noqa: BLE001
                pass
            try:
                rc = self.proc.wait(timeout=timeout)
                REGISTRY.discard(self.proc)
                self.exit = ServerExit(returncode=rc, signal=(-rc if rc < 0 else None), expected=(rc == 0),
                                       when=time.monotonic())
                self.exit_phase = "shutdown"
                self._close_out()
                return
            except subprocess.TimeoutExpired:
                pass
        self.expected_stop = True
        if self.proc.poll() is None:
            kill_group(self.proc, "mysqld", signal.SIGKILL)
        else:
            kill_group(self.proc, "mysqld", signal.SIGKILL, wait=1)
        self.exit = ServerExit(returncode=self.proc.returncode,
                               signal=(-self.proc.returncode if self.proc.returncode and self.proc.returncode < 0 else None),
                               expected=True, when=time.monotonic())
        REGISTRY.discard(self.proc)
        self._close_out()

    def _close_out(self) -> None:
        f = getattr(self, "stdout_f", None)
        if f is not None and not f.closed:
            f.close()

    def cleanup(self, keep_datadir: bool) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.expected_stop = True
            kill_group(self.proc, "mysqld")
            REGISTRY.discard(self.proc)
        self._close_out()
        for p in (self.socket, Path(str(self.socket) + ".lock"), self.client_cnf):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        if not keep_datadir:
            for d in (self.datadir, self.tmpdir):
                if d.exists():
                    safe_rmtree(d, self.owned_root)

    def read_segment(self, start: int, end: int | None) -> str:
        try:
            with open(self.error_log, "rb") as f:
                f.seek(start)
                data = f.read() if end is None else f.read(max(0, end - start))
            return data.decode("utf-8", "replace")
        except FileNotFoundError:
            return ""


def run_hook(argv: list[str], *, env_extra: dict, timeout: float, out_path: Path) -> tuple[str, str]:
    """Run a bounded hook; returns (status, detail).  status in ok|failed|timeout."""
    env = dict(os.environ)
    env.update({k: str(v) for k, v in env_extra.items()})
    with open(out_path, "wb") as out:
        try:
            proc = subprocess.Popen(argv, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                    env=env, start_new_session=True)
        except OSError as e:
            return "failed", f"cannot start hook {argv[0]}: {e}"
        REGISTRY.add(proc, f"hook {argv[0]}")
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            kill_group(proc, f"hook {argv[0]}")
            return "timeout", f"hook {argv[0]} exceeded {timeout}s"
        finally:
            REGISTRY.discard(proc)
    if rc != 0:
        return "failed", f"hook {argv[0]} exited with {rc}"
    return "ok", ""


def load_setup(server: ManagedServer, setup: dict, cfg: dict, password: str, timeout: float) -> tuple[bool, str]:
    """Create databases and load logical setup.  Returns (ok, detail)."""
    try:
        c = server.connect(password, timeout=cfg["trial"]["connect_timeout"], read_timeout=timeout)
        try:
            cur = c.cursor()
            for db in setup.get("create_databases") or []:
                cur.execute("CREATE DATABASE IF NOT EXISTS `" + db.replace("`", "``") + "`")
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001
        return False, f"cannot create setup databases: {e}"
    if setup["kind"] != "sql":
        return True, ""
    path = Path(setup["path"])
    if setup.get("sha256") and sha256_file(path) != setup["sha256"]:
        return False, f"setup file {path} changed since import (checksum mismatch)"
    if setup.get("loader", "mysql-cli") == "mysql-cli":
        client = cfg["server"]["mysql_client"]
        if not client:
            return False, "setup loader mysql-cli needs server.mysql_client (mysql binary)"
        server.write_client_cnf(password)
        argv = [client, f"--defaults-file={server.client_cnf}", "--binary-mode", "--default-character-set=utf8mb4"]
        out_path = server.attempt_dir / "setup.out"
        with open(path, "rb") as fin, open(out_path, "wb") as out:
            proc = subprocess.Popen(argv, stdin=fin, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
            REGISTRY.add(proc, "mysql setup")
            try:
                rc = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                kill_group(proc, "mysql setup")
                return False, f"setup load exceeded {timeout}s"
            finally:
                REGISTRY.discard(proc)
        if rc != 0:
            tail = out_path.read_text(errors="replace")[-2000:]
            return False, f"setup load failed (mysql rc={rc}): {tail}"
        return True, ""
    # driver loader: send the whole file as one multi-statement request
    try:
        import pymysql
        from pymysql.constants import CLIENT
        c = pymysql.connect(unix_socket=str(server.socket), user=cfg["server"]["user"], password=password,
                            connect_timeout=cfg["trial"]["connect_timeout"], read_timeout=timeout,
                            write_timeout=timeout, autocommit=True, charset="utf8mb4",
                            client_flag=CLIENT.MULTI_STATEMENTS)
        try:
            cur = c.cursor()
            cur.execute(path.read_bytes().decode("utf-8", "surrogateescape"))
            while cur.nextset():
                pass
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001
        return False, f"setup load (driver) failed: {e}"
    return True, ""


def copy_snapshot_source(setup: dict) -> Path:
    p = Path(setup["path"])
    if not p.is_dir():
        raise ReducerError(f"setup snapshot {p} not found", 4)
    return p
