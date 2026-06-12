# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Motion: UDP commands → /cmd_vel via ROS2.

This is the ONLY rclpy user in the system. Everything else uses direct
hardware access. The orchestrator calls rclpy.init() once at startup and
shares the global context with this controller.

Behavior:
    - Publishes geometry_msgs/Twist to /cmd_vel at motion_publish_hz.
    - Watchdog: if no command arrives within motion_watchdog_sec, output zero.
    - robot_lock=True → output zero regardless of incoming commands.
    - brake=True       → output zero regardless of incoming commands.
    - ang_z is multiplied by ang_z_scale (default 0.20) so turning feels
      proportional to forward speed. Matches the original.
    - Publishes 3× zero on stop() for safety.

The orchestrator passes parsed values into command() — this controller
does no UDP work and doesn't know about source arbitration.
"""

import threading
import time
from typing import Optional

from .common import log


class MotionController:
    def __init__(
        self,
        topic:            str   = "/cmd_vel",
        publish_hz:       int   = 50,
        watchdog_sec:     float = 0.30,
        ang_z_scale:      float = 0.20,
    ) -> None:
        self._topic         = topic
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

        # ROS2 handles
        self._node          = None
        self._pub           = None
        self._executor      = None
        self._executor_thread: Optional[threading.Thread] = None

        # Publisher loop
        self._stop          = threading.Event()
        self._pub_thread    = threading.Thread(target=self._publish_loop, daemon=True, name="motion-pub")

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Create the ROS2 node and start the publish loop.

        rclpy.init() must already have been called by the orchestrator.
        """
        try:
            import rclpy   # type: ignore
            from rclpy.executors import SingleThreadedExecutor   # type: ignore
            from geometry_msgs.msg import Twist                  # type: ignore

            if not rclpy.ok():
                log("motion", "rclpy not initialized — orchestrator must call rclpy.init() first")
                return

            self._node = rclpy.create_node("lab_motion")
            self._pub  = self._node.create_publisher(Twist, self._topic, 10)

            self._executor = SingleThreadedExecutor()
            self._executor.add_node(self._node)
            self._executor_thread = threading.Thread(
                target=self._executor.spin, daemon=True, name="motion-spin"
            )
            self._executor_thread.start()

            self._pub_thread.start()
            log("motion", f"publishing → {self._topic} @ {self._publish_hz} Hz "
                          f"(watchdog={self._watchdog*1000:.0f}ms, ang_scale={self._ang_z_scale})")
        except ImportError:
            log("motion", "rclpy not installed — motion disabled")
        except Exception as exc:
            log("motion", f"start failed: {exc}")

    def stop(self) -> None:
        self._stop.set()
        try:
            self._pub_thread.join(timeout=1.0)
        except Exception:
            pass

        # Publish 3× zero for safety on shutdown
        for _ in range(3):
            self._send_twist(0.0, 0.0)
            time.sleep(0.02)

        if self._executor is not None:
            try:
                self._executor.shutdown(timeout_sec=1.0)
            except Exception:
                pass
        if self._node is not None:
            try:
                self._node.destroy_node()
            except Exception:
                pass
        self._node = None
        self._pub  = None

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
        """Public read of (lin_x, ang_z, locked, braking). Used by stream badges and recorder."""
        with self._lock:
            return self._lin_x, self._ang_z, self._locked, self._braking

    # ── publisher loop ────────────────────────────────────────────────────────

    def _publish_loop(self) -> None:
        interval = 1.0 / self._publish_hz
        while not self._stop.is_set():
            lin, ang = self._compute_output()
            self._send_twist(lin, ang)
            self._stop.wait(timeout=interval)

    def _compute_output(self) -> tuple[float, float]:
        """Apply all safety gates and return (lin_x, ang_z) to publish this tick."""
        with self._lock:
            now          = time.monotonic()
            watchdog_ok  = (now - self._last_cmd_t) < self._watchdog
            locked       = self._locked
            braking      = self._braking
            lin_x        = self._lin_x
            ang_z        = self._ang_z * self._ang_z_scale

        if not watchdog_ok or locked or braking:
            return 0.0, 0.0
        return lin_x, ang_z

    def _send_twist(self, lin: float, ang: float) -> None:
        if self._pub is None:
            return
        try:
            from geometry_msgs.msg import Twist   # type: ignore
            t = Twist()
            t.linear.x  = float(lin)
            t.angular.z = float(ang)
            self._pub.publish(t)
        except Exception:
            pass