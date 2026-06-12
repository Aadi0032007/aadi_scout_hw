# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Shared helpers used across LAB modules.

Keep this file tiny — only utilities that genuinely belong in more than one
place. If something is used by exactly one file, leave it there.
"""

import time
from typing import Any, Iterable


# ── Type coercion ─────────────────────────────────────────────────────────────

def truthy(value: Any) -> bool:
    """Permissive truthiness check for UDP JSON values.

    Accepts bool, int/float, and the strings: 1/true/yes/on/pressed/down.
    Everything else (including None and unknown strings) returns False.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on", "pressed", "down")
    return False


def first_float(pkt: dict, keys: Iterable[str], default: float = 0.0) -> float:
    """Return the first value from `pkt` that matches one of `keys` and parses as float."""
    for key in keys:
        v = pkt.get(key)
        if v is None:
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return default


def first_int(pkt: dict, keys: Iterable[str], default: int = 0) -> int:
    """Same as first_float but returns int."""
    for key in keys:
        v = pkt.get(key)
        if v is None:
            continue
        try:
            return int(v)
        except (TypeError, ValueError):
            continue
    return default


# ── Time helpers ──────────────────────────────────────────────────────────────

def now_mono() -> float:
    """Monotonic clock for measuring intervals. Never goes backward."""
    return time.monotonic()


def now_unix() -> float:
    """Wall-clock time. Use only for human-readable timestamps and dataset rows."""
    return time.time()


# ── Logging ───────────────────────────────────────────────────────────────────

def log(tag: str, msg: str) -> None:
    """Single-line tagged log to stdout. Used everywhere instead of `print`."""
    print(f"[{tag}] {msg}", flush=True)