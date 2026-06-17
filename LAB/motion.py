# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


# -*- coding: utf-8 -*-
"""
Motion: UDP commands → Docker ROS1 /cmd_vel via UDP forward.

Previously this module used rclpy to publish directly to a local ROS2 node.
The Segway SmartCar SDK has moved into a ROS1 Docker container
(segway_ros1), so we now forward motion commands as JSON UDP packets to
revo_docker_udp_motion_keepalive.py running inside that container.

The Docker script listens on UDP port 55999 inside the container; the
container is port-mapped so host port 56000 → container 55999. We send
to host port 56000. (Host port 55999 is already in use by the gamepad
listener, so we can't reuse it.)

The Docker script applies its own deadzone/limits and publishes /cmd_vel
to ROS1 at 50 Hz with keepalive. We run our own publish loop here at
motion_publish_hz so the Docker side always receives a steady stream
and its watchdog (HOLD_LAST_CMD_S=0.40s) never trips during normal
operation.

Public API is IDENTICAL to the rclpy version — teleop.py call site
updated only to pass docker_host/docker_port instead of topic; record.py
is untouched:
    command(lin_x, ang_z, locked, braking)   ← called by UDP dispatcher
    state()           → raw pre-gate values  ← used by stream overlay
    published_state() → post-gate values     ← used by recorder
    start() / stop()

Behavior preserved from rclpy version:
    - Watchdog: zero output if no command within motion_watchdog_sec.
    - robot_lock=True  → zero output.
    - brake=True       → zero output.
    - ang_z multiplied by ang_z_scale (default 0.20).
    - Sends 3× zero on stop() for safety.

Wire protocol:
    JSON  {"lin_x": <float>, "ang_z": <float>}
    UDP   127.0.0.1:56000  (default; override via docker_host/docker_port)
"""

import json
import socket
import threading
import time
from typing import Optional

from .common import log


class MotionController:
    def __init__(
        self,
        docker_host:      str   = "127.0.0.1",
        docker_port:      int   = 56000,
        publish_hz:       int   = 50,
        watchdog_sec:     float = 0.30,
        ang_z_scale:      float = 0.20,
    ) -> None:
        self._docker_host   = docker_host
        self._docker_port   = docker_port
        self._publish_hz    = max(1, publish_hz)
        self._watchdog      = watchdog_sec
        self._ang_z_scale   = ang_z_scale

        # State (protected by _lock)
        self._lock          = threading.Lock()
        self._lin_x         = 0.0
        self._ang_z         = 0.0
        self._locked        = True   # start locked for safety
        self._braking       = False
        self._last_cmd_t    = 0.0

        # Last values actually sent by the publish loop.
        # Updated synchronously — no ROS subscription timing involved.
        # published_state() exposes these to the recorder.
        self._last_pub_lin: float = 0.0
        self._last_pub_ang: float = 0.0

        # UDP socket (send-only, reused across ticks)
        self._sock: Optional[socket.socket] = None

        # Publisher loop
        self._stop          = threading.Event()
        self._pub_thread    = threading.Thread(
            target=self._publish_loop, daemon=True, name="motion-pub"
        )

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Open the UDP socket and start the publish loop."""
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._pub_thread.start()
            log(
                "motion",
                f"forwarding → udp://{self._docker_host}:{self._docker_port} "
                f"@ {self._publish_hz} Hz "
                f"(watchdog={self._watchdog*1000:.0f}ms, ang_scale={self._ang_z_scale})"
            )
        except Exception as exc:
            log("motion", f"start failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._pub_thread.join(timeout=1.0)
        except Exception:
            pass

        # Send 3× zero for safety so the Docker keepalive ramps to zero
        for _ in range(3):
            self._send_twist(0.0, 0.0)
            time.sleep(0.02)

        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    # ── public API ────────────────────────────────────────────────────────────

    def command(self, lin_x: float, ang_z: float, locked: bool, braking: bool) -> None:
        """Update the latest commanded state. Called from the UDP dispatcher."""
        with self._lock:
            self._lin_x      = float(lin_x)
            self._ang_z      = float(ang_z)
            self._locked     = bool(locked)
            self._braking    = bool(braking)
            self._last_cmd_t = time.monotonic()

    def state(self) -> tuple[float, float, bool, bool]:
        """Raw pre-gate state: (lin_x, ang_z, locked, braking).

        This is what the teleoperator commanded before ang_z_scale,
        watchdog, lock, and brake are applied. Used for the stream's
        speed-badge overlay where showing operator intent is appropriate.

        Do NOT pass this to the recorder. Use published_state() there.
        """
        with self._lock:
            return self._lin_x, self._ang_z, self._locked, self._braking

    def published_state(self) -> tuple[float, float]:
        """Return the last (linear_x, angular_z) actually forwarded to Docker.

        Updated synchronously in _publish_loop right after _send_twist,
        so it always reflects what the robot received: post ang_z_scale,
        post watchdog, post lock/brake.

        Pass THIS to the recorder, not state().
        """
        with self._lock:
            return self._last_pub_lin, self._last_pub_ang

    # ── publisher loop ────────────────────────────────────────────────────────

    def _publish_loop(self) -> None:
        interval = 1.0 / self._publish_hz
        while not self._stop.is_set():
            lin, ang = self._compute_output()
            self._send_twist(lin, ang)

            # Store synchronously so published_state() always reflects
            # what we just forwarded, without any subscription timing.
            with self._lock:
                self._last_pub_lin = lin
                self._last_pub_ang = ang

            self._stop.wait(timeout=interval)

    def _compute_output(self) -> tuple[float, float]:
        """Apply all safety gates and return (lin_x, ang_z) to send this tick."""
        with self._lock:
            now         = time.monotonic()
            watchdog_ok = (now - self._last_cmd_t) < self._watchdog
            locked      = self._locked
            braking     = self._braking
            lin_x       = self._lin_x
            ang_z       = self._ang_z * self._ang_z_scale

        if not watchdog_ok or locked or braking:
            return 0.0, 0.0
        return lin_x, ang_z

    def _send_twist(self, lin: float, ang: float) -> None:
        """Send a JSON UDP packet to the Docker ROS1 bridge."""
        if self._sock is None:
            return
        try:
            print(f"sending {lin},{ang}")
            payload = json.dumps({"lin_x": lin, "ang_z": ang}).encode()
            self._sock.sendto(payload, (self._docker_host, self._docker_port))
        except Exception:
            pass