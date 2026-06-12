# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Sensor readers — IMU (WIT/JY901 over UART) and GPS (NMEA over UART).

Both read serial ports directly. No journalctl, no ROS2 subscriptions,
no external service dependencies. Each runs in a background daemon thread
and exposes a latest-snapshot via get().

IMU frames are 11 bytes: 0x55, frame_id, 8 payload, checksum.
GPS sentences are standard NMEA + Unicore UM982 #ADRNAVA extensions.
"""

import math
import threading
import time
from typing import Optional
import socket as _socket


from .common import log

try:
    import serial   # type: ignore
    _HAS_SERIAL = True
except ImportError:
    _HAS_SERIAL = False


# ═══ IMU ══════════════════════════════════════════════════════════════════════

# WIT protocol constants
_IMU_FRAME_LEN = 11
_IMU_SYNC      = 0x55
_IMU_KNOWN_IDS = {0x51, 0x52, 0x53, 0x54, 0x59}   # accel, gyro, RPY, mag, quat


def _imu_checksum_ok(frame: bytes) -> bool:
    return (sum(frame[:10]) & 0xFF) == frame[10]


def _imu_int16(lo: int, hi: int) -> int:
    v = (hi << 8) | lo
    return v - 0x10000 if v >= 0x8000 else v


class ImuReader:
    """Reads WIT/JY901-style binary frames directly from a UART."""

    def __init__(self, port: str, baud: int = 9600) -> None:
        self._port = port
        self._baud = baud
        self._data: dict = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="imu-reader")

    def start(self) -> None:
        if not _HAS_SERIAL:
            log("sensors", "pyserial not installed — IMU disabled")
            return
        self._thread.start()
        log("sensors", f"IMU reader started ({self._port} @ {self._baud})")

    def get(self) -> dict:
        with self._lock:
            return dict(self._data)

    def stop(self) -> None:
        self._stop.set()

    # ── internals ─────────────────────────────────────────────────────────────

    def _run(self) -> None:
        backoff = 1.0
        buf = bytearray()

        while not self._stop.is_set():
            ser = self._open()
            if ser is None:
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            backoff = 1.0
            buf.clear()

            try:
                while not self._stop.is_set():
                    chunk = ser.read(ser.in_waiting or 1)
                    if not chunk:
                        continue
                    buf.extend(chunk)
                    self._drain_frames(buf)
            except Exception as exc:
                log("sensors", f"IMU read error: {exc}")
            finally:
                try:
                    ser.close()
                except Exception:
                    pass

    def _open(self):
        try:
            return serial.Serial(
                port=self._port,
                baudrate=self._baud,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                timeout=0.05,
            )
        except Exception as exc:
            log("sensors", f"IMU open failed {self._port}: {exc}")
            return None

    def _drain_frames(self, buf: bytearray) -> None:
        """Parse all complete frames from buf, leaving any partial tail."""
        while len(buf) >= _IMU_FRAME_LEN:
            # Find sync byte
            if buf[0] != _IMU_SYNC:
                buf.pop(0)
                continue
            frame = bytes(buf[:_IMU_FRAME_LEN])
            if not _imu_checksum_ok(frame):
                buf.pop(0)
                continue
            self._decode(frame)
            del buf[:_IMU_FRAME_LEN]

    def _decode(self, frame: bytes) -> None:
        fid = frame[1]
        if fid not in _IMU_KNOWN_IDS:
            return
        d = frame[2:10]
        upd: dict = {}

        if fid == 0x51:   # accelerometer in g (range ±16g)
            upd["accelerometer_x"] = _imu_int16(d[0], d[1]) / 32768.0 * 16.0
            upd["accelerometer_y"] = _imu_int16(d[2], d[3]) / 32768.0 * 16.0
            upd["accelerometer_z"] = _imu_int16(d[4], d[5]) / 32768.0 * 16.0
        elif fid == 0x52: # gyroscope in deg/s (range ±2000 dps)
            upd["gyroscope_x"] = _imu_int16(d[0], d[1]) / 32768.0 * 2000.0
            upd["gyroscope_y"] = _imu_int16(d[2], d[3]) / 32768.0 * 2000.0
            upd["gyroscope_z"] = _imu_int16(d[4], d[5]) / 32768.0 * 2000.0
        elif fid == 0x53: # roll/pitch/yaw in degrees (range ±180°)
            upd["roll"]  = _imu_int16(d[0], d[1]) / 32768.0 * 180.0
            upd["pitch"] = _imu_int16(d[2], d[3]) / 32768.0 * 180.0
            upd["yaw"]   = _imu_int16(d[4], d[5]) / 32768.0 * 180.0
        elif fid == 0x54: # magnetometer raw counts
            upd["magnetometer_x"] = float(_imu_int16(d[0], d[1]))
            upd["magnetometer_y"] = float(_imu_int16(d[2], d[3]))
            upd["magnetometer_z"] = float(_imu_int16(d[4], d[5]))
        elif fid == 0x59: # quaternion (normalized -1..+1)
            upd["quat_w"] = _imu_int16(d[0], d[1]) / 32768.0
            upd["quat_x"] = _imu_int16(d[2], d[3]) / 32768.0
            upd["quat_y"] = _imu_int16(d[4], d[5]) / 32768.0
            upd["quat_z"] = _imu_int16(d[6], d[7]) / 32768.0

        if upd:
            with self._lock:
                self._data.update(upd)


# ═══ GPS ══════════════════════════════════════════════════════════════════════


class GpsReader:
    """Reads NMEA + UM982 #ADRNAVA sentences from a UDP socket fed by gps_mux."""

    _FIX_LABEL = {
        0: "NO_FIX", 1: "GPS_FIX", 2: "DGPS_FIX",
        4: "RTK_FIXED", 5: "RTK_FLOAT", 6: "ESTIMATED",
    }

    def __init__(self, udp_host: str = "127.0.0.1", udp_port: int = 57002) -> None:
        self._host = udp_host
        self._port = udp_port
        self._data: dict = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sock: Optional[_socket.socket] = None
        self._thread = threading.Thread(target=self._run, daemon=True, name="gps-reader")

    def start(self) -> None:
        self._thread.start()
        log("sensors", f"GPS reader started (udp://{self._host}:{self._port})")

    def get(self) -> dict:
        with self._lock:
            return dict(self._data)

    def stop(self) -> None:
        self._stop.set()
        if self._sock is not None:
            try: self._sock.close()
            except Exception: pass

    # ── internals ─────────────────────────────────────────────────────────────

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
                s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
                s.bind((self._host, self._port))
                s.settimeout(0.5)
                self._sock = s
            except OSError as exc:
                log("sensors", f"GPS UDP bind {self._host}:{self._port} failed: {exc}")
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 10.0)
                continue
            backoff = 1.0

            try:
                while not self._stop.is_set():
                    try:
                        data, _ = s.recvfrom(2048)
                    except _socket.timeout:
                        continue
                    if not data:
                        continue
                    line = data.decode("ascii", errors="ignore").strip()
                    if line:
                        self._parse(line)
            except Exception as exc:
                log("sensors", f"GPS recv error: {exc}")
            finally:
                try: s.close()
                except Exception: pass
                self._sock = None

    # ── parser (UNCHANGED from previous version) ──────────────────────────────

    def _parse(self, line: str) -> None:
        if line.startswith("$"):
            core = line[1:].split("*", 1)[0]
            parts = core.split(",")
            if not parts:
                return
            msg = parts[0]
            if   msg.endswith("GGA"): self._parse_gga(parts)
            elif msg.endswith("RMC"): self._parse_rmc(parts)
            elif msg.endswith("VTG"): self._parse_vtg(parts)
            elif msg.endswith("HDT"): self._parse_hdt(parts)
        elif line.startswith("#ADRNAVA"):
            self._parse_adrnava(line)

    @staticmethod
    def _dm_to_decimal(value: str, hemi: str) -> Optional[float]:
        if not value:
            return None
        try:
            raw = float(value)
        except ValueError:
            return None
        deg = int(raw / 100)
        minutes = raw - (deg * 100)
        dec = deg + (minutes / 60.0)
        return -dec if hemi in ("S", "W") else dec

    @staticmethod
    def _to_float(v: str) -> Optional[float]:
        try: return float(v)
        except (ValueError, TypeError): return None

    @staticmethod
    def _to_int(v: str) -> Optional[int]:
        try: return int(v)
        except (ValueError, TypeError): return None

    def _parse_gga(self, parts: list) -> None:
        if len(parts) < 12: return
        upd: dict = {}
        lat = self._dm_to_decimal(parts[2], parts[3])
        lon = self._dm_to_decimal(parts[4], parts[5])
        fix = self._to_int(parts[6])
        if lat is not None: upd["gps_latitude"]  = lat
        if lon is not None: upd["gps_longitude"] = lon
        if fix is not None:
            upd["gps_status"] = fix
            upd["gps_fix"]    = self._FIX_LABEL.get(fix, "UNKNOWN")
        sats = self._to_int(parts[7]);  alt = self._to_float(parts[9])
        hdop = self._to_float(parts[8])
        if sats is not None: upd["gps_satellites"] = sats
        if hdop is not None: upd["gps_hdop"]       = hdop
        if alt  is not None: upd["gps_altitude"]   = alt
        self._merge(upd)

    def _parse_rmc(self, parts: list) -> None:
        if len(parts) < 10: return
        upd: dict = {}
        lat = self._dm_to_decimal(parts[3], parts[4])
        lon = self._dm_to_decimal(parts[5], parts[6])
        sog = self._to_float(parts[7]); cog = self._to_float(parts[8])
        if lat is not None: upd["gps_latitude"]  = lat
        if lon is not None: upd["gps_longitude"] = lon
        if sog is not None:
            upd["gps_speed_knots"] = sog
            upd["gps_speed_kmh"]   = round(sog * 1.852, 3)
        if cog is not None:
            upd["gps_cog"] = cog
            self._set_default("orientation", cog)
        self._merge(upd)

    def _parse_vtg(self, parts: list) -> None:
        if len(parts) < 9: return
        upd: dict = {}
        cog     = self._to_float(parts[1])
        spd_kmh = self._to_float(parts[7])
        if cog is not None:
            upd["gps_cog"] = cog
            self._set_default("orientation", cog)
        if spd_kmh is not None:
            upd["gps_speed_kmh"] = spd_kmh
        self._merge(upd)

    def _parse_hdt(self, parts: list) -> None:
        if len(parts) < 2: return
        hdg = self._to_float(parts[1])
        if hdg is not None:
            with self._lock:
                self._data["orientation"]      = hdg
                self._data["heading_deg_true"] = hdg

    def _parse_adrnava(self, line: str) -> None:
        body = line[1:].split("*", 1)[0]
        if ";" in body:
            _, payload = body.split(";", 1)
        else:
            payload = ""
        p = payload.split(",") if payload else []
        upd: dict = {}
        if len(p) > 0 and p[0]: upd["gps_solution_status"] = p[0]
        if len(p) > 1 and p[1]: upd["gps_position_type"]   = p[1]
        if upd:
            self._merge(upd)

    def _merge(self, upd: dict) -> None:
        with self._lock:
            self._data.update(upd)

    def _set_default(self, key: str, value: float) -> None:
        with self._lock:
            # Only use COG for orientation if we DON'T have a True Heading yet.
            if "heading_deg_true" not in self._data:
                self._data[key] = value