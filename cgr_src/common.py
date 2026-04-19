"""Shared imports, version metadata, terminal formatting, and utility helpers."""
from __future__ import annotations

import argparse, codecs, datetime, errno, fcntl, hashlib, hmac, io, json, os, pty, re, secrets, select, selectors, shlex, signal, stat, subprocess, sys, tempfile, termios, textwrap, threading, time, tty, warnings
from contextlib import nullcontext, redirect_stdout
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from pathlib import Path
from typing import Any

# Engine release version. Update through the process in RELEASE.md.
__version__ = "0.6.0"

def _duration_to_secs(value: int, suffix) -> int:
    if suffix == "m": return value * 60
    if suffix == "h": return value * 3600
    return value

def _parse_duration_str(s: str) -> int:
    import re as _re
    m = _re.fullmatch(r"(\d+)(s|m|h)?", s.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid duration: {s!r} (expected e.g. 300, 300s, 5m, 2h)")
    return _duration_to_secs(int(m.group(1)), m.group(2))

def _parse_timeout_text(text: str) -> tuple[int, bool] | None:
    m = re.search(r'\btimeout\s+(\d+)(s|m|h)?(?:\s+reset\s+on\s+output)?', text)
    if not m:
        return None
    timeout = _duration_to_secs(int(m.group(1)), m.group(2))
    return timeout, ("reset on output" in m.group(0))

# ═══════════════════════════════════════════════════════════════════════════════
# Terminal colours
# ═══════════════════════════════════════════════════════════════════════════════
_COLOR = sys.stdout.isatty()
def _c(code: str, t: str) -> str: return f"\033[{code}m{t}\033[0m" if _COLOR else t
def green(t):   return _c("32", t)
def blue(t):    return _c("34", t)
def red(t):     return _c("31", t)
def yellow(t):  return _c("33", t)
def dim(t):     return _c("2", t)
def bold(t):    return _c("1", t)
def cyan(t):    return _c("36", t)
def magenta(t): return _c("35", t)
_ANSI_RE = re.compile(r'\033\[[0-9;]*m')
def _strip_ansi(t: str) -> str: return _ANSI_RE.sub('', t)
def _html_esc(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#x27;"))

def _include_path_parts(include_path: str) -> tuple[str, ...]:
    if any(ch in include_path for ch in "\x00\r\n"):
        raise ValueError("include path contains an invalid character")
    if "\\" in include_path:
        raise ValueError("include path must use '/' separators")
    if include_path.startswith("/") or re.match(r"^[A-Za-z]:[/\\]", include_path):
        raise ValueError("include path must be relative to the graph directory")
    if include_path.startswith("~"):
        raise ValueError("include path must not use home-directory expansion")
    path_parts = tuple(part for part in include_path.split("/") if part not in ("", "."))
    if not path_parts:
        raise ValueError("include path must not be empty")
    if ".." in path_parts:
        raise ValueError("include path escapes graph directory")
    return path_parts

def _include_base_dir(current_filename: str = "") -> Path:
    raise ValueError("include requires a graph file path")

def _resolve_include_path(include_path: str, current_filename: str = "", base_dir: Path | None = None) -> Path:
    """Resolve an include path without allowing it to escape the graph directory."""
    path_parts = _include_path_parts(include_path)
    include_base = base_dir.resolve() if base_dir is not None else _include_base_dir(current_filename)
    candidate = include_base.joinpath(*path_parts).resolve()
    if os.path.commonpath([str(include_base), str(candidate)]) != str(include_base):
        raise ValueError(f"include path escapes graph directory: {include_path}")
    return candidate

def _read_include_file(include_path: str, current_filename: str = "", base_dir: Path | None = None) -> tuple[Path, str]:
    """Read a validated include file from inside the graph directory."""
    path_parts = _include_path_parts(include_path)
    include_base = base_dir.resolve() if base_dir is not None else _include_base_dir(current_filename)
    resolved = _resolve_include_path(include_path, current_filename, include_base)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    dir_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | nofollow
    file_flags = os.O_RDONLY | nofollow

    dir_fd = os.open(include_base, dir_flags)
    try:
        for part in path_parts[:-1]:
            try:
                next_fd = os.open(part, dir_flags, dir_fd=dir_fd)
            except OSError as exc:
                if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                    raise ValueError("include path escapes graph directory") from exc
                raise
            os.close(dir_fd)
            dir_fd = next_fd

        try:
            file_fd = os.open(path_parts[-1], file_flags, dir_fd=dir_fd)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("include path escapes graph directory") from exc
            raise
        try:
            if not stat.S_ISREG(os.fstat(file_fd).st_mode):
                raise FileNotFoundError(str(resolved))
            with os.fdopen(file_fd, "r", encoding="utf-8") as f:
                file_fd = None
                return resolved, f.read()
        except Exception:
            if file_fd is not None:
                os.close(file_fd)
            raise
    finally:
        os.close(dir_fd)

__all__ = [
    "Any",
    "Enum",
    "Path",
    "ThreadPoolExecutor",
    "argparse",
    "as_completed",
    "auto",
    "codecs",
    "cyan",
    "blue",
    "bold",
    "dataclass",
    "datetime",
    "defaultdict",
    "deque",
    "dim",
    "errno",
    "fcntl",
    "field",
    "green",
    "hashlib",
    "hmac",
    "io",
    "json",
    "magenta",
    "nullcontext",
    "os",
    "pty",
    "red",
    "re",
    "redirect_stdout",
    "replace",
    "secrets",
    "select",
    "selectors",
    "shlex",
    "signal",
    "stat",
    "subprocess",
    "sys",
    "tempfile",
    "termios",
    "textwrap",
    "threading",
    "time",
    "tty",
    "warnings",
    "yellow",
    "__version__",
    "_ANSI_RE",
    "_COLOR",
    "_c",
    "_duration_to_secs",
    "_html_esc",
    "_parse_duration_str",
    "_parse_timeout_text",
    "_read_include_file",
    "_resolve_include_path",
    "_strip_ansi",
]
