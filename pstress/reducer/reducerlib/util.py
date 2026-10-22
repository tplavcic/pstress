"""Small shared helpers: hashing, atomic files, JSON, safe cleanup, redaction."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

TOOL_NAME = "pstress_reducer"
TOOL_VERSION = "0.1.0"


class ReducerError(Exception):
    """An expected, user-facing error (bad config, bad input, refused action)."""

    def __init__(self, message: str, exit_code: int = 2):
        super().__init__(message)
        self.exit_code = exit_code


# ---------------------------------------------------------------------------
# Exit codes (documented in README.md)
# ---------------------------------------------------------------------------
EXIT_OK = 0                      # verified result / successful command
EXIT_INTERNAL = 1                # unexpected internal error
EXIT_USAGE = 2                   # usage or configuration error
EXIT_BASELINE_UNCONFIRMED = 3    # target not observed on the unreduced input
EXIT_INVALID_RUNS = 4            # repeated infrastructure / invalid trials
EXIT_BUDGET_PARTIAL = 5          # budget exhausted; partial (confirmed) best saved
EXIT_FINAL_VALIDATION_FAILED = 6  # final validation failed; fell back / unvalidated
EXIT_IMPORT_NEEDS_CORRECTION = 7  # importer found records it cannot frame
EXIT_INTERRUPTED = 130           # Ctrl+C / SIGTERM


# ---------------------------------------------------------------------------
# JSON / hashing
# ---------------------------------------------------------------------------
def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "surrogateescape")).hexdigest()


def sha256_obj(obj: Any) -> str:
    return sha256_text(canonical_json(obj))


def sha256_file(path: str | os.PathLike, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def sha256_tree(root: str | os.PathLike, mode: str = "content") -> str:
    """Checksum a directory tree.

    mode="content" hashes relative path + file bytes (slow for big datadirs but exact).
    mode="metadata" hashes relative path + size + mtime_ns (fast, weaker).
    Symlinks are hashed by their target text, never followed.
    """
    root = Path(root)
    h = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            p = Path(dirpath) / name
            rel = p.relative_to(root).as_posix()
            h.update(b"F\0" + rel.encode("utf-8", "surrogateescape") + b"\0")
            if p.is_symlink():
                h.update(b"L" + os.readlink(p).encode("utf-8", "surrogateescape"))
                continue
            st = p.stat()
            if mode == "metadata":
                h.update(f"{st.st_size}:{st.st_mtime_ns}".encode())
            else:
                h.update(sha256_file(p).encode())
    return h.hexdigest()


def read_json(path: str | os.PathLike) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def atomic_write_text(path: str | os.PathLike, text: str) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", errors="surrogateescape") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        try:
            dfd = os.open(str(path.parent), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def atomic_write_json(path: str | os.PathLike, obj: Any, indent: int | None = 2) -> None:
    atomic_write_text(path, json.dumps(obj, indent=indent, sort_keys=False, ensure_ascii=True) + "\n")


def append_jsonl(path: str | os.PathLike, obj: Any) -> None:
    line = json.dumps(obj, ensure_ascii=True, sort_keys=True) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def iter_jsonl(path: str | os.PathLike) -> Iterable[Any]:
    """Yield JSON objects; a torn final line (crash mid-write) is ignored."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        return


# ---------------------------------------------------------------------------
# Paths and cleanup
# ---------------------------------------------------------------------------
def is_within(path: str | os.PathLike, root: str | os.PathLike) -> bool:
    p = os.path.realpath(path)
    r = os.path.realpath(root)
    return p == r or p.startswith(r.rstrip(os.sep) + os.sep)


def safe_rmtree(path: str | os.PathLike, owned_root: str | os.PathLike) -> None:
    """Remove a directory only if it lies strictly inside the owned root."""
    p = os.path.realpath(path)
    r = os.path.realpath(owned_root)
    if p == r or not is_within(p, r):
        raise ReducerError(f"refusing to delete {p}: not inside owned work directory {r}", 1)
    if os.path.lexists(p):
        shutil.rmtree(p, ignore_errors=True)


def copy_tree_no_links(src: str | os.PathLike, dst: str | os.PathLike) -> None:
    """Copy a directory with real file copies (never hard links to the source)."""
    shutil.copytree(src, dst, symlinks=True, copy_function=shutil.copy2)


def dir_size(path: str | os.PathLike) -> int:
    total = 0
    for dp, _dn, fns in os.walk(path):
        for fn in fns:
            try:
                total += os.lstat(os.path.join(dp, fn)).st_size
            except OSError:
                pass
    return total


# ---------------------------------------------------------------------------
# Redaction / logging
# ---------------------------------------------------------------------------
_SECRETS: list[str] = []


def register_secret(value: str | None) -> None:
    if value and len(value) >= 1 and value not in _SECRETS:
        _SECRETS.append(value)


def redact(text: str) -> str:
    for s in _SECRETS:
        if s:
            text = text.replace(s, "*****")
    text = re.sub(r"(--password=)\S+", r"\1*****", text)
    return text


def redact_argv(argv: list[str]) -> list[str]:
    return [redact(a) for a in argv]


_LOG_FILE: Path | None = None
_QUIET = False


def set_log_file(path: str | os.PathLike | None, quiet: bool = False) -> None:
    global _LOG_FILE, _QUIET
    _LOG_FILE = Path(path) if path else None
    _QUIET = quiet


def log(msg: str, level: str = "INFO") -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {redact(msg)}"
    if not _QUIET or level in ("WARN", "ERROR"):
        print(line, file=sys.stderr, flush=True)
    if _LOG_FILE is not None:
        try:
            with open(_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass


def warn(msg: str) -> None:
    log(msg, "WARN")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")
