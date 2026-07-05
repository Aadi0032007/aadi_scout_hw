# -*- coding: utf-8 -*-
"""
Created on Sun Jul  5 07:07:19 2026

@author: Aadi
"""
from __future__ import annotations

"""
udp_telemetry.py — plain UDP telemetry to a fixed endpoint.

Runs in parallel with AzureTelemetry (dashboard WS + IoT Hub). This is the
cheap, no-framing, drops-are-fine path — one JSON packet per tick to a
Tailscale VM IP. Useful for a downstream consumer that just wants a
firehose of state and doesn't need durability or acks.

Snapshot API is intentionally the same as AzureTelemetry — same callables,
same expected sensor keys — so teleop can build one shared set of snapshot
functions and hand both classes the same references.
"""

import json
import socket
import threading
import time
from typing import Any, Callable, Optional

from .common import log


class UdpTelemetryPublisher:
    def __init__(
        self,
        host:               str,
        port:               int,
        hz:                 int,
        robot_id:           str,
        motion_state_fn:    Callable[[], tuple],
        published_state_fn: Callable[[], tuple],
        speed_label_fn:     Callable[[], Optional[str]],
        ai_enabled_fn:      Callable[[], bool],
        sensor_snapshot_fn: Optional[Callable[[], dict]] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._period = 1.0 / max(1, hz)
        self._robot_id = robot_id

        self._motion_state    = motion_state_fn
        self._published_state = published_state_fn
        self._speed_label     = speed_label_fn
        self._ai_enabled      = ai_enabled_fn
        self._sensor_snapshot = sensor_snapshot_fn or (lambda: {})

        self._sock: Optional[socket.socket] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._seq = 0

    def start(self) -> None:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        except Exception as exc:
            log("udp_tel", f"socket create failed: {exc} — disabled")
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="udp-telemetry"
        )
        self._thread.start()
        log("udp_tel",
            f"publishing → udp://{self._host}:{self._port} @ {1.0/self._period:.0f} Hz")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _run(self) -> None:
        while not self._stop.is_set():
            t0 = time.time()
            try:
                pkt = self._build_packet(t0)
                if self._sock is not None:
                    self._sock.sendto(
                        json.dumps(pkt, separators=(",", ":")).encode("utf-8"),
                        (self._host, self._port),
                    )
            except Exception as exc:
                # Non-fatal — try again next tick.
                log("udp_tel", f"send error: {exc}")
            elapsed = time.time() - t0
            self._stop.wait(timeout=max(0.0, self._period - elapsed))

    def _build_packet(self, t: float) -> dict:
        self._seq += 1
        pkt: dict[str, Any] = {
            "robot_id": self._robot_id,
            "t":        t,
            "seq":      self._seq,
        }

        try:
            lin_x, ang_z, locked, braking = self._motion_state()
            pkt["lin_x_cmd"]  = round(float(lin_x), 4)
            pkt["ang_z_cmd"]  = round(float(ang_z), 4)
            pkt["robot_lock"] = bool(locked)
            pkt["braking"]    = bool(braking)
        except Exception:
            pass

        try:
            lin_pub, ang_pub = self._published_state()
            pkt["lin_x_out"] = round(float(lin_pub), 4)
            pkt["ang_z_out"] = round(float(ang_pub), 4)
        except Exception:
            pass

        try:
            label = self._speed_label()
            if label:
                pkt["speed_label"] = str(label)
        except Exception:
            pass

        try:
            pkt["ai_enabled"] = bool(self._ai_enabled())
        except Exception:
            pass

        try:
            sensors = self._sensor_snapshot() or {}
            for k in ("battery_v", "temperature_c", "humidity_pct",
                      "lat", "lon", "alt_m", "gps_fix", "orientation_deg"):
                if sensors.get(k) is not None:
                    pkt[k] = sensors[k]
        except Exception:
            pass

        return pkt