# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
heartbeat.py — POST to streams.revobots.ai/api/robots/register every 60s.

Without this the fleet doesn't know the robot exists; the browser can't find
it and no WS control connection can be established.

Adapted from util_robot_heartbeat.py — same POST payload, same cadence,
same tailscale-ip resolution with configured-value fallback.

To avoid one log line every 60s when healthy, we only log:
    - once at startup (start())
    - on transitions ok → fail  and  fail → ok
    - every failure (so you notice)
"""

import json
import subprocess
import threading
import urllib.request
from typing import Optional

from .common import log


class HeartbeatPublisher:
    def __init__(
        self,
        register_url:  str,
        robot_id:      str,
        pilot_camera:  str,
        camera_names:  list,
        interval_sec:  int = 60,
        ip_fallback:   str = "",
    ) -> None:
        self._url          = register_url
        self._robot_id     = robot_id
        self._pilot_camera = pilot_camera
        self._cameras      = list(camera_names)
        self._interval     = max(5, int(interval_sec))
        self._ip_fallback  = ip_fallback

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_ok: Optional[bool] = None   # tri-state: None until first attempt

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="heartbeat"
        )
        self._thread.start()
        log("heartbeat",
            f"registering {self._robot_id} → {self._url} every {self._interval}s")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    # ── internals ───────────────────────────────────────────────────────────

    def _tailscale_ip(self) -> str:
        """Return the robot's tailscale IPv4, or the configured fallback."""
        try:
            out = subprocess.check_output(
                ["tailscale", "ip", "-4"], text=True, timeout=5
            ).strip()
            # tailscale may print multiple addresses; take the first non-empty.
            first = next((ln.strip() for ln in out.splitlines() if ln.strip()), "")
            return first or self._ip_fallback
        except Exception:
            return self._ip_fallback

    def _post_once(self) -> bool:
        body = {
            "robot_id":     self._robot_id,
            "room_name":    self._robot_id,
            "tailscale_ip": self._tailscale_ip(),
            "pilot_camera": self._pilot_camera,
            "cameras":      self._cameras,
        }
        req = urllib.request.Request(
            self._url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
            return True
        except Exception as exc:
            log("heartbeat", f"register failed: {exc}")
            return False

    def _run(self) -> None:
        while not self._stop.is_set():
            ok = self._post_once()
            if ok and self._last_ok is not True:
                log("heartbeat", "registered ok")
            self._last_ok = ok
            self._stop.wait(timeout=self._interval)