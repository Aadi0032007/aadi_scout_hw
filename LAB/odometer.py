# -*- coding: utf-8 -*-
"""Jetson-owned cumulative odometer.

Advances from Segway chassis meter deltas (positive only) and persists to
disk so teleop restarts and Segway meter resets do not wipe lifetime miles.
Seed is 0 on first create — historical Segway miles are intentionally ignored.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional

from .common import log

DEFAULT_PATH = Path.home() / ".cache" / "scout" / "odometer.json"
METERS_TO_MILES = 0.621371 / 1000.0


class JetsonOdometer:
    """Cumulative distance owned by the Jetson (meters + miles).

    State file:
      {"odometer_m": float, "last_chassis_m": float|null}
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self._path = Path(path) if path is not None else DEFAULT_PATH
        self._lock = threading.Lock()
        self._odometer_m: float = 0.0
        self._last_chassis_m: Optional[float] = None
        self._dirty = False
        self._load()

    def _load(self) -> None:
        try:
            if not self._path.is_file():
                return
            data = json.loads(self._path.read_text())
            self._odometer_m = max(0.0, float(data.get("odometer_m", 0.0)))
            last = data.get("last_chassis_m", None)
            self._last_chassis_m = None if last is None else float(last)
            log("odometer",
                f"loaded {self._odometer_m:.1f} m "
                f"({self.miles():.2f} mi) from {self._path}")
        except Exception as exc:
            log("odometer", f"load failed ({exc}) — starting at 0")
            self._odometer_m = 0.0
            self._last_chassis_m = None

    def _save_unlocked(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "odometer_m": self._odometer_m,
                "last_chassis_m": self._last_chassis_m,
            }
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2) + "\n")
            os.replace(tmp, self._path)
            self._dirty = False
        except Exception as exc:
            log("odometer", f"save failed ({exc})")

    def update_from_chassis_meters(self, chassis_m: float) -> float:
        """Apply a Segway get_vehicle_meter() reading. Returns odometer_m."""
        chassis_m = float(chassis_m)
        with self._lock:
            if self._last_chassis_m is None:
                # First reading after deploy / empty file: latch only.
                self._last_chassis_m = chassis_m
                self._dirty = True
                self._save_unlocked()
                return self._odometer_m

            if chassis_m >= self._last_chassis_m:
                delta = chassis_m - self._last_chassis_m
                if delta > 0.0:
                    self._odometer_m += delta
                    self._dirty = True
                self._last_chassis_m = chassis_m
            else:
                # Segway reset / backward jump — rebase, do not subtract.
                log("odometer",
                    f"chassis meter reset/jump "
                    f"{self._last_chassis_m:.0f} -> {chassis_m:.0f} m "
                    f"— rebasing (kept {self._odometer_m:.1f} m)")
                self._last_chassis_m = chassis_m
                self._dirty = True

            if self._dirty:
                self._save_unlocked()
            return self._odometer_m

    def meters(self) -> float:
        with self._lock:
            return self._odometer_m

    def miles(self) -> float:
        with self._lock:
            return round(self._odometer_m * METERS_TO_MILES, 2)
