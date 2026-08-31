# -*- coding: utf-8 -*-
"""
Lidar reader — RPLIDAR S2/S2L over UART, background-thread + snapshot API.

Same pattern as ImuReader and GpsReader in sensors.py: a daemon thread keeps
the serial connection alive, parses Slamtec SCAN measurements, and updates a
latest-snapshot dict. Consumers call get() to retrieve sector distances and
a "blocked" flag derived from configurable bubble thresholds.

Protocol portions adapted from util_lidar_driver.py (Slamtec UART SCAN mode
at 1 Mbps). The sampling loop follows util_lidar_values.py: start_scan() ONCE,
then repeated read_points_for() windows over a live stream — re-issuing the
stop/reset/SCAN handshake every poll never lets the motor reach steady RPM.

The standalone-script scaffolding (env file, argparse, main loop, print_status)
is intentionally not included here — those concerns now live in config.py and
teleop.py.
"""

from __future__ import annotations

import glob
import os
import struct
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from .common import log, now_mono

try:
    import serial  # type: ignore
    _HAS_SERIAL = True
except ImportError:
    _HAS_SERIAL = False


# ═══ Slamtec UART protocol constants ══════════════════════════════════════════

_SYNC_BYTE = 0xA5
_CMD_STOP = 0x25
_CMD_RESET = 0x40
_CMD_SCAN = 0x20
_MEASUREMENT_LEN = 5


class LidarError(Exception):
    pass


@dataclass
class ScanPoint:
    angle_deg: float
    distance_m: float
    quality: int


# ═══ Port discovery helpers ═══════════════════════════════════════════════════

def _list_tty_usb_ports() -> List[str]:
    return sorted(glob.glob("/dev/ttyUSB*"))


def _usb_serial_for_port(port: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["udevadm", "info", "-q", "property", "-n", port],
            capture_output=True, text=True, timeout=2, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    for line in result.stdout.splitlines():
        if line.startswith("ID_SERIAL_SHORT="):
            return line.split("=", 1)[1]
    return None


def _find_port_by_usb_serial(usb_serial: str) -> Optional[str]:
    for port in _list_tty_usb_ports():
        if _usb_serial_for_port(port) == usb_serial:
            return port
    return None


def resolve_lidar_port(
    symlink: str = "/dev/rplidar_s2",
    usb_serial: str = "",
    configured_port: str = "",
) -> Optional[str]:
    """Pick the lidar serial port from udev symlink, USB-serial id, or fallback."""
    if symlink:
        if Path(symlink).exists():
            return symlink

    if usb_serial:
        matched = _find_port_by_usb_serial(usb_serial)
        if matched:
            return matched

    if configured_port and Path(configured_port).exists():
        port_serial = _usb_serial_for_port(configured_port)
        if port_serial and usb_serial and port_serial != usb_serial:
            log("lidar",
                f"{configured_port} exists but USB_SERIAL={port_serial} "
                f"!= expected {usb_serial}")
            return None
        return configured_port

    return None


# ═══ Geometry helpers ═════════════════════════════════════════════════════════

def _normalize_angle_deg(angle: float) -> float:
    while angle <= -180.0:
        angle += 360.0
    while angle > 180.0:
        angle -= 360.0
    return angle


def _angle_in_range(angle_deg: float, min_deg: float, max_deg: float) -> bool:
    angle_deg = _normalize_angle_deg(angle_deg)
    min_deg = _normalize_angle_deg(min_deg)
    max_deg = _normalize_angle_deg(max_deg)
    if min_deg <= max_deg:
        return min_deg <= angle_deg <= max_deg
    return angle_deg >= min_deg or angle_deg <= max_deg


def _min_distance_in_sector(
    points: Iterable[ScanPoint],
    min_deg: float, max_deg: float,
    range_min: float, range_max: float,
    min_quality: int,
) -> float:
    best = float("inf")
    for p in points:
        if p.quality < min_quality:
            continue
        if not (range_min < p.distance_m < range_max):
            continue
        if _angle_in_range(p.angle_deg, min_deg, max_deg):
            best = min(best, p.distance_m)
    return best


# ═══ Slamtec serial driver (SCAN mode) ════════════════════════════════════════

class _RPLidarS2:
    """Minimal Slamtec serial driver for SCAN mode (S2 @ 1 Mbps)."""

    def __init__(self, port: str, baudrate: int, timeout: float = 1.0) -> None:
        self.port = port
        self.baudrate = baudrate
        self._serial = serial.Serial(
            port=port,
            baudrate=baudrate,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            bytesize=serial.EIGHTBITS,
            timeout=timeout,
        )
        # Prefer short read timeouts so streaming loops can poll the deadline.
        try:
            self._serial.timeout = min(float(timeout), 0.05)
        except Exception:
            pass

    def close(self) -> None:
        """Stop scanning/motor and close the port without restarting the motor.

        RPLIDAR USB adapters run the motor when DTR is low. Closing the serial
        port normally deasserts DTR (HUPCL), which starts the motor again.
        We clear HUPCL and leave DTR asserted so the motor stays off after exit.
        """
        if not self._serial.is_open:
            return
        try:
            self.stop()
        except Exception:
            pass
        try:
            self.stop_motor()
            time.sleep(0.05)
        except Exception:
            pass
        try:
            self._leave_dtr_asserted_on_close()
        except Exception:
            pass
        try:
            self._serial.close()
        except Exception:
            pass

    def _leave_dtr_asserted_on_close(self) -> None:
        """Prevent Linux from dropping DTR when the last fd is closed."""
        try:
            import termios
        except ImportError:
            return
        fd = self._serial.fileno()
        attrs = termios.tcgetattr(fd)
        # attrs: [iflag, oflag, cflag, lflag, ispeed, ospeed, cc]
        attrs[2] &= ~termios.HUPCL
        termios.tcsetattr(fd, termios.TCSANOW, attrs)
        # Motor off = DTR asserted (high) on Slamtec USB adapters.
        self._serial.dtr = True

    def shutdown(self) -> None:
        """Stop scan + motor and close (safe to call from finally blocks)."""
        self.close()

    def _send_command(self, cmd: int) -> None:
        self._serial.write(bytes([_SYNC_BYTE, cmd, cmd]))  # checksum = cmd ^ 0

    def stop(self) -> None:
        self._send_command(_CMD_STOP)
        time.sleep(0.05)

    def reset(self) -> None:
        self._send_command(_CMD_RESET)
        time.sleep(0.5)

    def start_motor(self) -> None:
        self._serial.dtr = False

    def stop_motor(self) -> None:
        self._serial.dtr = True

    @staticmethod
    def _parse_measurement(raw: bytes) -> Tuple[bool, int, float, float]:
        if len(raw) != _MEASUREMENT_LEN:
            raise LidarError(f"Bad measurement length: {len(raw)}")
        new_scan = bool(raw[0] & 0x01)
        inv_new_scan = bool((raw[0] >> 1) & 0x01)
        # Slamtec: S and /S must be complements. Same bits => invalid sample.
        if new_scan == inv_new_scan:
            raise LidarError("New scan flag mismatch")
        quality = raw[0] >> 2
        angle_deg = ((raw[1] >> 1) | (raw[2] << 7)) / 64.0
        distance_m = (raw[3] | (raw[4] << 8)) / 4000.0
        if angle_deg > 360.0 or distance_m < 0.0:
            raise LidarError("Out-of-range measurement")
        return new_scan, quality, angle_deg, distance_m

    @staticmethod
    def _find_next_measurement(raw_buf: bytearray) -> Optional[bytes]:
        while len(raw_buf) >= _MEASUREMENT_LEN:
            chunk = bytes(raw_buf[:_MEASUREMENT_LEN])
            try:
                _RPLidarS2._parse_measurement(chunk)
                return chunk
            except LidarError:
                del raw_buf[0]
        return None

    def start_scan(self) -> None:
        """Enter SCAN mode once and spin the motor (keep streaming)."""
        self.stop()
        time.sleep(0.1)
        self.reset()
        self.stop()
        time.sleep(0.1)
        self._serial.reset_input_buffer()
        self._send_command(_CMD_SCAN)
        try:
            header = self._serial.read(2)
            if header == bytes([_SYNC_BYTE, 0x5A]):
                self._serial.read(5)
        except Exception:
            pass
        self.start_motor()
        time.sleep(0.5)

    def read_points_for(
        self,
        duration_sec: float,
        *,
        raw_buf: Optional[bytearray] = None,
        max_points: int = 50000,
        stop_event: Optional[threading.Event] = None,
    ) -> List[ScanPoint]:
        """Read all valid samples for duration_sec (no new_scan gating).

        Prefer this for stable sector stats: ~0.5-1.0 s is several full
        revolutions at ~10 Hz. raw_buf is owned by the caller and carries
        partial measurements across calls — the byte stream is continuous, so
        dropping it between windows would resync for nothing.

        stop_event is a local addition (the driver script has no equivalent):
        this runs on a daemon thread, and without it teleop shutdown stalls
        for up to a full sample window.
        """
        if raw_buf is None:
            raw_buf = bytearray()
        deadline = time.monotonic() + max(0.05, float(duration_sec))
        points: List[ScanPoint] = []
        while time.monotonic() < deadline and len(points) < max_points:
            if stop_event is not None and stop_event.is_set():
                break
            # Pull more bytes only when we don't already have a full sample.
            if len(raw_buf) < _MEASUREMENT_LEN:
                chunk_in = self._serial.read(512)
                if chunk_in:
                    raw_buf.extend(chunk_in)
                elif not raw_buf:
                    continue
            # Drain every complete/valid measurement currently buffered.
            drained = False
            while len(points) < max_points:
                chunk = self._find_next_measurement(raw_buf)
                if chunk is None:
                    break
                drained = True
                del raw_buf[:_MEASUREMENT_LEN]
                _new_scan, quality, angle_deg, distance_m = self._parse_measurement(
                    chunk
                )
                points.append(
                    ScanPoint(
                        angle_deg=_normalize_angle_deg(angle_deg),
                        distance_m=distance_m,
                        quality=quality,
                    )
                )
            if not drained and time.monotonic() < deadline:
                # No aligned sample yet — pull more bytes.
                chunk_in = self._serial.read(512)
                if chunk_in:
                    raw_buf.extend(chunk_in)
        if not points:
            raise LidarError("No scan points received (check power, port, baud)")
        return points


# ═══ Public reader ════════════════════════════════════════════════════════════

class LidarReader:
    """Background lidar reader. Same shape as ImuReader / GpsReader."""

    def __init__(
        self,
        *,
        port: str = "",
        symlink: str = "/dev/rplidar_s2",
        usb_serial: str = "",
        baud: int = 1_000_000,
        poll_hz: float = 2.0,
        scan_timeout_sec: float = 3.0,
        sample_sec: float = 0.8,
        range_min: float = 0.05,
        range_max: float = 18.0,
        min_quality: int = 0,
        front_min_deg: float = -30.0,
        front_max_deg: float = 30.0,
        left_min_deg: float = 60.0,
        left_max_deg: float = 120.0,
        right_min_deg: float = -120.0,
        right_max_deg: float = -60.0,
        bubble_front_m: float = 0.10,
        bubble_left_m: float = 0.10,
        bubble_right_m: float = 0.10,
        stale_after_sec: float = 2.0,
    ) -> None:
        self._port_configured = port
        self._symlink = symlink
        self._usb_serial = usb_serial
        self._baud = baud
        self._poll_interval = 1.0 / poll_hz if poll_hz > 0 else 0.5
        # Retained for API compatibility. The streaming loop paces on
        # sample_sec + poll_interval, so nothing consults this any more.
        self._scan_timeout = scan_timeout_sec
        self._sample_sec = sample_sec
        self._range_min = range_min
        self._range_max = range_max
        self._min_quality = min_quality
        self._sectors = {
            "front": (front_min_deg, front_max_deg, bubble_front_m),
            "left":  (left_min_deg,  left_max_deg,  bubble_left_m),
            "right": (right_min_deg, right_max_deg, bubble_right_m),
        }
        self._stale_after = stale_after_sec

        self._data: dict = {"lidar_status": "starting"}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="lidar-reader")

    def start(self) -> None:
        if not _HAS_SERIAL:
            log("lidar", "pyserial not installed — lidar disabled")
            with self._lock:
                self._data["lidar_status"] = "no_serial"
            return
        self._thread.start()
        log("lidar",
            f"lidar reader started (baud={self._baud} "
            f"poll_hz={1.0/self._poll_interval:.1f} "
            f"sample={self._sample_sec:.2f}s)")

    def get(self) -> dict:
        with self._lock:
            snapshot = dict(self._data)
        ts = snapshot.get("lidar_ts_mono")
        if ts is not None:
            snapshot["lidar_age_sec"] = max(0.0, now_mono() - ts)
        return snapshot

    def _fresh(self) -> Optional[dict]:
        """Snapshot if the scan is usable, else None (bad status or stale)."""
        snap = self.get()
        if snap.get("lidar_status") != "ok":
            return None
        if snap.get("lidar_age_sec", 999.0) > self._stale_after:
            return None
        return snap

    def is_blocked(self) -> bool:
        """Any sector inside its bubble + scan is fresh."""
        snap = self._fresh()
        if snap is None:
            return False
        return bool(snap.get("lidar_blocked", False))

    def is_blocked_forward(self, commanded_lin_x: Optional[float]) -> bool:
        """Forward-only brake: blocks if commanded forward AND front bubble fires."""
        if commanded_lin_x is None or commanded_lin_x <= 0.0:
            return False
        snap = self._fresh()
        if snap is None:
            return False
        return bool(snap.get("lidar_blocked_front", False))

    def is_blocked_cmd(
        self,
        commanded_lin_x: Optional[float],
        commanded_ang_z: Optional[float] = None,
    ) -> bool:
        """Directional brake: front blocks forward, left/right block turns.

        Mirrors print_status() in util_lidar_driver.py (STOP if front OR left
        OR right is inside its bubble), but only against the sectors the
        command actually drives into — so a wall on one side cannot freeze
        the robot in place with no way to drive out of it.
        """
        snap = self._fresh()
        if snap is None:
            return False

        if (commanded_lin_x is not None and commanded_lin_x > 0.0
                and snap.get("lidar_blocked_front", False)):
            return True
        if commanded_ang_z is not None:
            # ROS convention: +ang_z is a left (counter-clockwise) turn.
            if commanded_ang_z > 0.0 and snap.get("lidar_blocked_left", False):
                return True
            if commanded_ang_z < 0.0 and snap.get("lidar_blocked_right", False):
                return True
        return False

    def stop(self) -> None:
        self._stop.set()

    # ── internals ────────────────────────────────────────────────────────────

    def _resolve_port(self) -> Optional[str]:
        return resolve_lidar_port(
            symlink=self._symlink,
            usb_serial=self._usb_serial,
            configured_port=self._port_configured,
        )

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            port = self._resolve_port()
            if port is None:
                with self._lock:
                    self._data["lidar_status"] = "no_port"
                log("lidar",
                    f"no port found (symlink={self._symlink}, "
                    f"usb_serial={self._usb_serial})")
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 10.0)
                continue

            try:
                driver = _RPLidarS2(port=port, baudrate=self._baud, timeout=1.0)
            except Exception as exc:
                log("lidar", f"open {port} failed: {exc}")
                with self._lock:
                    self._data["lidar_status"] = "open_failed"
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 10.0)
                continue

            log("lidar", f"connected on {port}")
            backoff = 1.0
            raw_buf = bytearray()

            try:
                driver.start_scan()
                # Motor needs a moment after DTR spin-up before dense samples
                # arrive; prime once and throw away whatever lands.
                self._stop.wait(timeout=1.0)
                try:
                    driver.read_points_for(
                        0.3, raw_buf=raw_buf, stop_event=self._stop)
                except LidarError:
                    pass

                while not self._stop.is_set():
                    loop_start = time.monotonic()
                    try:
                        points = driver.read_points_for(
                            self._sample_sec,
                            raw_buf=raw_buf,
                            stop_event=self._stop,
                        )
                        self._update_from_scan(points)
                    except LidarError as exc:
                        log("lidar", f"scan error: {exc}")
                        with self._lock:
                            self._data["lidar_status"] = "scan_error"
                        # Re-enter SCAN on the SAME port rather than dropping
                        # the connection: the byte stream is what went out of
                        # sync, not the device.
                        try:
                            driver.stop()
                            time.sleep(0.3)
                            driver.start_scan()
                            raw_buf.clear()
                        except Exception:
                            break

                    elapsed = time.monotonic() - loop_start
                    sleep_for = self._poll_interval - elapsed
                    if sleep_for > 0:
                        self._stop.wait(timeout=sleep_for)
            except Exception as exc:
                log("lidar", f"reader loop error: {exc}")
                with self._lock:
                    self._data["lidar_status"] = "error"
            finally:
                try:
                    driver.shutdown()
                except Exception:
                    pass

    def _update_from_scan(self, points: List[ScanPoint]) -> None:
        dists: dict = {}
        any_blocked = False
        per_sector_blocked: dict = {}

        for name, (min_d, max_d, bubble) in self._sectors.items():
            d = _min_distance_in_sector(
                points, min_d, max_d,
                self._range_min, self._range_max, self._min_quality,
            )
            dists[f"lidar_{name}_m"] = None if d == float("inf") else round(d, 3)
            blocked = (d != float("inf")) and (d < bubble)
            per_sector_blocked[f"lidar_blocked_{name}"] = blocked
            any_blocked = any_blocked or blocked

        upd = {
            "lidar_status": "ok",
            "lidar_ts_mono": now_mono(),
            "lidar_point_count": len(points),
            "lidar_blocked": any_blocked,
        }
        upd.update(dists)
        upd.update(per_sector_blocked)

        with self._lock:
            self._data.update(upd)
