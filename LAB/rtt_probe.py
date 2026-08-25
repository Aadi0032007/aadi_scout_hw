# -*- coding: utf-8 -*-
"""Outbound RTT probe for cloud telemetry.

Default target is the Tailscale cloud peer used for video/telemetry
(100.94.48.1 — same LAN as MediaMTX pull path), measured with ICMP ping so
we still get a number when MediaMTX does not accept inbound TCP from the
robot.

TCP connect mode remains available via config (method=\"tcp\") for hosts that
listen on a port.

Snapshot callers just read the cache. On failure/stale, rtt_ms is omitted
from get() so the dashboard treats it like missing cellular_sinr.
"""
from __future__ import annotations

import re
import socket
import subprocess
import threading
import time
from typing import Optional

from .common import log

_PING_TIME_RE = re.compile(r"time[=<]([\d.]+)\s*ms", re.IGNORECASE)


class RttProbe:
    """Background RTT probe (ICMP ping or TCP connect)."""

    def __init__(
        self,
        host: str = "100.94.48.1",
        port: int = 443,
        method: str = "icmp",
        poll_sec: float = 5.0,
        timeout_sec: float = 3.0,
        stale_after_sec: float = 30.0,
    ) -> None:
        self._host = str(host).strip()
        self._port = int(port)
        method_n = str(method or "icmp").strip().lower()
        self._method = method_n if method_n in ("icmp", "tcp") else "icmp"
        self._poll_sec = max(1.0, float(poll_sec))
        self._timeout_sec = float(timeout_sec)
        self._stale_after_sec = float(stale_after_sec)

        self._rtt_ms: Optional[float] = None
        self._last_ok_t: float = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="rtt-probe")
        self._fail_log_t: float = 0.0

    def start(self) -> None:
        if not self._host:
            log("rtt", "host unset — RTT probe disabled")
            return
        self._thread.start()
        if self._method == "tcp":
            target = f"tcp://{self._host}:{self._port}"
        else:
            target = f"icmp://{self._host}"
        log(
            "rtt",
            f"probe started ({target}, poll={self._poll_sec:.0f}s)",
        )

    def get(self) -> dict:
        with self._lock:
            rtt = self._rtt_ms
            last_ok = self._last_ok_t
        out: dict = {}
        if last_ok > 0:
            age = time.monotonic() - last_ok
            out["age_sec"] = age
            if rtt is not None and age <= self._stale_after_sec:
                out["rtt_ms"] = rtt
        return out

    def stop(self) -> None:
        self._stop.set()

    def _log_fail(self, exc: BaseException) -> None:
        now = time.monotonic()
        if now - self._fail_log_t >= 30.0:
            log("rtt", f"probe failed: {exc}")
            self._fail_log_t = now

    def _measure_tcp(self) -> Optional[float]:
        t0 = time.perf_counter()
        try:
            with socket.create_connection(
                (self._host, self._port),
                timeout=self._timeout_sec,
            ):
                pass
        except OSError as exc:
            self._log_fail(exc)
            return None
        return round((time.perf_counter() - t0) * 1000.0, 1)

    def _measure_icmp(self) -> Optional[float]:
        # One echo; -W is deadline seconds (iputils on Linux).
        wait_s = max(1, int(round(self._timeout_sec)))
        try:
            proc = subprocess.run(
                ["ping", "-n", "-c", "1", "-W", str(wait_s), self._host],
                capture_output=True,
                text=True,
                timeout=wait_s + 2.0,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            self._log_fail(exc)
            return None
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or f"ping exit {proc.returncode}").strip()
            self._log_fail(RuntimeError(err[:160] or "ping failed"))
            return None
        m = _PING_TIME_RE.search(proc.stdout or "")
        if not m:
            self._log_fail(RuntimeError("ping ok but no time= in output"))
            return None
        return round(float(m.group(1)), 1)

    def _measure_once(self) -> Optional[float]:
        if self._method == "tcp":
            return self._measure_tcp()
        return self._measure_icmp()

    def _run(self) -> None:
        while not self._stop.is_set():
            ms = self._measure_once()
            if ms is not None:
                with self._lock:
                    self._rtt_ms = ms
                    self._last_ok_t = time.monotonic()
            self._stop.wait(timeout=self._poll_sec)