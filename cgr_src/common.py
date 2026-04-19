"""Shared imports, version metadata, terminal formatting, and utility helpers."""
from __future__ import annotations

import argparse, codecs, datetime, fcntl, hashlib, hmac, io, json, os, pty, re, secrets, select, selectors, shlex, signal, subprocess, sys, tempfile, termios, textwrap, threading, time, tty, warnings
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

def _resolve_include_path(include_path: str, current_filename: str = "") -> Path:
    """Resolve an include path without allowing it to escape the graph directory."""
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
    if current_filename and not (current_filename.startswith("<") and current_filename.endswith(">")):
        base_dir = Path(current_filename).resolve().parent
    else:
        base_dir = Path.cwd().resolve()
    candidate = base_dir.joinpath(*path_parts).resolve()
    if os.path.commonpath([str(base_dir), str(candidate)]) != str(base_dir):
        raise ValueError(f"include path escapes graph directory: {include_path}")
    return candidate

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
    "_resolve_include_path",
    "_strip_ansi",
]
