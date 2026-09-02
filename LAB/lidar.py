# -*- coding: utf-8 -*-
"""
Lidar reader — RPLIDAR S2/S2L over UART, background-thread + snapshot API.

Same pattern as ImuReader and GpsReader in sensors.py: a daemon thread keeps
the serial connection alive, parses Slamtec SCAN measurements, and updates a
latest-snapshot dict. Consumers call get() to retrieve sector distances and
a "blocked" flag derived from configurable bubble thresholds.

Protocol portions adapted from util_lidar_driver.py (Slamtec UART: legacy SCAN +
express DenseBoost for S2). The sampling loop follows util_lidar_values.py: start_scan() ONCE,
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
from typing import Dict, Iterable, List, Optional, Tuple

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
_CMD_EXPRESS_SCAN = 0x82
_CMD_HQ_SCAN = 0x83
_CMD_GET_LIDAR_CONF = 0x84
_CMDFLAG_HAS_PAYLOAD = 0x80

_ANS_TYPE_MEASUREMENT = 0x81
_ANS_TYPE_MEASUREMENT_CAPSULED = 0x82
_ANS_TYPE_MEASUREMENT_HQ = 0x83
_ANS_TYPE_GET_LIDAR_CONF = 0x84
_ANS_TYPE_MEASUREMENT_DENSE_CAPSULED = 0x85
_ANS_TYPE_MEASUREMENT_ULTRA_DENSE_CAPSULED = 0x86

_CONF_SCAN_MODE_TYPICAL = 0x0000007C
_CONF_SCAN_MODE_ANS_TYPE = 0x00000075

_EXP_SYNC_1 = 0xA
_EXP_SYNC_2 = 0x5
_CAPSULE_PACKET_LEN = 84
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


class BubbleDebouncer:
    """Engage bubble brake after N consecutive blocked polls; release on first clear."""

    def __init__(self, confirm_polls: int = 2) -> None:
        self._confirm = max(1, confirm_polls)
        self._strikes: Dict[str, int] = {}
        self._engaged: Dict[str, bool] = {}

    def update(self, sector: str, raw_blocked: bool) -> bool:
        if raw_blocked:
            strikes = self._strikes.get(sector, 0) + 1
            self._strikes[sector] = strikes
            if strikes >= self._confirm:
                self._engaged[sector] = True
        else:
            self._strikes[sector] = 0
            self._engaged[sector] = False
        return self._engaged.get(sector, False)


# ═══ Scan decoders (legacy 5-byte + Slamtec express capsules) ════════════════

class _BaseScanDecoder:
    def reset(self) -> None:
        pass

    def feed(self, data: bytes) -> List[ScanPoint]:
        raise NotImplementedError


class _LegacyScanDecoder(_BaseScanDecoder):
    def __init__(self) -> None:
        self._buf = bytearray()

    def reset(self) -> None:
        self._buf.clear()

    def feed(self, data: bytes) -> List[ScanPoint]:
        self._buf.extend(data)
        points: List[ScanPoint] = []
        while True:
            chunk = _RPLidarS2._find_next_measurement(self._buf)
            if chunk is None:
                break
            del self._buf[:_MEASUREMENT_LEN]
            _new_scan, quality, angle_deg, distance_m = _RPLidarS2._parse_measurement(
                chunk
            )
            points.append(
                ScanPoint(
                    angle_deg=_normalize_angle_deg(angle_deg),
                    distance_m=distance_m,
                    quality=quality,
                )
            )
        return points


class _CapsuleScanDecoder(_BaseScanDecoder):
    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0
        self._prev: Optional[bytes] = None
        self._prev_ready = False

    def reset(self) -> None:
        self._buf.clear()
        self._pos = 0
        self._prev = None
        self._prev_ready = False

    @staticmethod
    def _checksum_ok(packet: bytes) -> bool:
        recv = (packet[0] & 0xF) | ((packet[1] & 0xF) << 4)
        calc = 0
        for i in range(2, _CAPSULE_PACKET_LEN):
            calc ^= packet[i]
        return recv == calc

    def _decode_pair(self, cur: bytes) -> List[ScanPoint]:
        if not self._prev_ready or self._prev is None:
            return []
        if not self._checksum_ok(self._prev) or not self._checksum_ok(cur):
            return []
        prev = self._prev
        start_cur = struct.unpack_from("<H", cur, 2)[0]
        start_prev = struct.unpack_from("<H", prev, 2)[0]
        if start_cur & 0x8000:
            return []
        current_start_q8 = (start_cur & 0x7FFF) << 2
        prev_start_q8 = (start_prev & 0x7FFF) << 2
        diff_q8 = current_start_q8 - prev_start_q8
        if prev_start_q8 > current_start_q8:
            diff_q8 += 360 << 8
        angle_inc_q16 = diff_q8 << 3
        current_angle_q16 = prev_start_q8 << 8
        points: List[ScanPoint] = []
        for pos in range(16):
            off = 4 + pos * 5
            da1 = struct.unpack_from("<H", prev, off)[0]
            da2 = struct.unpack_from("<H", prev, off + 2)[0]
            off_angles = prev[off + 4]
            for dist_angle, nibble in ((da1, off_angles & 0xF), (da2, off_angles >> 4)):
                dist_q2 = dist_angle & 0xFFFC
                if dist_q2 == 0:
                    current_angle_q16 += angle_inc_q16
                    continue
                angle_offset_q3 = nibble | ((dist_angle & 0x3) << 4)
                angle_q6 = (current_angle_q16 - (angle_offset_q3 << 13)) >> 10
                current_angle_q16 += angle_inc_q16
                if angle_q6 < 0:
                    angle_q6 += 360 << 6
                if angle_q6 >= 360 << 6:
                    angle_q6 -= 360 << 6
                points.append(
                    ScanPoint(
                        angle_deg=_normalize_angle_deg(angle_q6 / 64.0),
                        distance_m=dist_q2 / 4000.0,
                        quality=0x2F,
                    )
                )
        return points

    def feed(self, data: bytes) -> List[ScanPoint]:
        self._buf.extend(data)
        points: List[ScanPoint] = []
        while self._pos < len(self._buf):
            b = self._buf[self._pos]
            if self._pos == 0:
                if (b >> 4) != _EXP_SYNC_1:
                    self._pos += 1
                    self._prev_ready = False
                    continue
            elif self._pos == 1:
                if (b >> 4) != _EXP_SYNC_2:
                    self._pos = 0
                    self._prev_ready = False
                    continue
            self._pos += 1
            if self._pos == _CAPSULE_PACKET_LEN:
                packet = bytes(self._buf[:_CAPSULE_PACKET_LEN])
                del self._buf[:_CAPSULE_PACKET_LEN]
                self._pos = 0
                points.extend(self._decode_pair(packet))
                self._prev = packet
                self._prev_ready = True
        return points


class _DenseCapsuleScanDecoder(_BaseScanDecoder):
    def __init__(self) -> None:
        self._buf = bytearray()
        self._pos = 0
        self._prev: Optional[bytes] = None
        self._prev_ready = False
        self._last_sync_bit = 0

    def reset(self) -> None:
        self._buf.clear()
        self._pos = 0
        self._prev = None
        self._prev_ready = False
        self._last_sync_bit = 0

    @staticmethod
    def _checksum_ok(packet: bytes) -> bool:
        recv = (packet[0] & 0xF) | ((packet[1] & 0xF) << 4)
        calc = 0
        for i in range(2, _CAPSULE_PACKET_LEN):
            calc ^= packet[i]
        return recv == calc

    def _decode_pair(self, cur: bytes) -> List[ScanPoint]:
        if not self._prev_ready or self._prev is None:
            return []
        if not self._checksum_ok(self._prev) or not self._checksum_ok(cur):
            return []
        prev = self._prev
        start_cur = struct.unpack_from("<H", cur, 2)[0]
        start_prev = struct.unpack_from("<H", prev, 2)[0]
        if start_cur & 0x8000:
            self._last_sync_bit = 0
            return []
        current_start_q8 = (start_cur & 0x7FFF) << 2
        prev_start_q8 = (start_prev & 0x7FFF) << 2
        diff_q8 = current_start_q8 - prev_start_q8
        if prev_start_q8 > current_start_q8:
            diff_q8 += 360 << 8
        angle_inc_q16 = (diff_q8 << 8) // 40
        if angle_inc_q16 <= 0:
            return []
        current_angle_q16 = prev_start_q8 << 8
        last_sync = self._last_sync_bit
        points: List[ScanPoint] = []
        for pos in range(40):
            dist_raw = struct.unpack_from("<H", prev, 4 + pos * 2)[0]
            dist_q2 = dist_raw << 2
            if dist_q2 == 0:
                current_angle_q16 += angle_inc_q16
                continue
            angle_q6 = current_angle_q16 >> 10
            sync_bit = (
                1
                if ((current_angle_q16 + angle_inc_q16) % (360 << 16))
                < (angle_inc_q16 << 1)
                else 0
            )
            sync_bit = (sync_bit ^ last_sync) & sync_bit
            current_angle_q16 += angle_inc_q16
            if angle_q6 < 0:
                angle_q6 += 360 << 6
            if angle_q6 >= 360 << 6:
                angle_q6 -= 360 << 6
            points.append(
                ScanPoint(
                    angle_deg=_normalize_angle_deg(angle_q6 / 64.0),
                    distance_m=dist_q2 / 4000.0,
                    quality=0x2F,
                )
            )
            last_sync = sync_bit
        self._last_sync_bit = last_sync
        return points

    def feed(self, data: bytes) -> List[ScanPoint]:
        self._buf.extend(data)
        points: List[ScanPoint] = []
        while self._pos < len(self._buf):
            b = self._buf[self._pos]
            if self._pos == 0:
                if (b >> 4) != _EXP_SYNC_1:
                    self._pos += 1
                    self._prev_ready = False
                    continue
            elif self._pos == 1:
                if (b >> 4) != _EXP_SYNC_2:
                    self._pos = 0
                    self._prev_ready = False
                    continue
            self._pos += 1
            if self._pos == _CAPSULE_PACKET_LEN:
                packet = bytes(self._buf[:_CAPSULE_PACKET_LEN])
                del self._buf[:_CAPSULE_PACKET_LEN]
                self._pos = 0
                points.extend(self._decode_pair(packet))
                self._prev = packet
                self._prev_ready = True
        return points


def _make_scan_decoder(scan_format: str) -> _BaseScanDecoder:
    if scan_format == "capsule":
        return _CapsuleScanDecoder()
    if scan_format == "dense":
        return _DenseCapsuleScanDecoder()
    return _LegacyScanDecoder()


def _ans_type_to_format(ans_type: int) -> str:
    if ans_type == _ANS_TYPE_MEASUREMENT:
        return "legacy"
    if ans_type == _ANS_TYPE_MEASUREMENT_CAPSULED:
        return "capsule"
    if ans_type == _ANS_TYPE_MEASUREMENT_DENSE_CAPSULED:
        return "dense"
    if ans_type == _ANS_TYPE_MEASUREMENT_ULTRA_DENSE_CAPSULED:
        return "ultra_dense"
    if ans_type == _ANS_TYPE_MEASUREMENT_HQ:
        return "hq"
    return "dense"


# ═══ Slamtec serial driver ════════════════════════════════════════════════════

class _RPLidarS2:
    """Slamtec serial driver for RPLIDAR S2/S2L (legacy SCAN + express modes)."""

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
        self._scan_format = "legacy"
        self._express_mode_id = 0
        self._decoder: _BaseScanDecoder = _LegacyScanDecoder()
        try:
            self._serial.timeout = min(float(timeout), 0.05)
        except Exception:
            pass

    @property
    def scan_mode_label(self) -> str:
        if self._scan_format == "legacy":
            return "legacy SCAN (0x20)"
        return f"express {self._scan_format} (mode_id={self._express_mode_id})"

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

    def _send_command(self, cmd: int, payload: bytes = b"") -> None:
        if not payload:
            checksum = cmd
            packet = bytes([_SYNC_BYTE, cmd, checksum])
        else:
            cmd_flag = cmd | _CMDFLAG_HAS_PAYLOAD
            checksum = cmd_flag
            for byte in payload:
                checksum ^= byte
            packet = (
                bytes([_SYNC_BYTE, cmd_flag, len(payload)])
                + payload
                + bytes([checksum])
            )
        self._serial.write(packet)

    def _read_exact(self, size: int) -> bytes:
        data = self._serial.read(size)
        if len(data) != size:
            raise LidarError(f"Timeout reading {size} bytes from {self.port}")
        return data

    def _read_descriptor(self) -> Tuple[bytes, int]:
        descriptor = self._read_exact(7)
        if descriptor[0] != _SYNC_BYTE or descriptor[1] != 0x5A:
            raise LidarError(f"Bad descriptor sync: {descriptor[:2]!r}")
        payload_len = struct.unpack("<I", descriptor[2:6])[0] & 0x3FFFFFFF
        return descriptor, payload_len

    def _command_with_response(
        self,
        cmd: int,
        payload: bytes,
        expected_ans_type: int,
        *,
        timeout: float = 1.0,
    ) -> bytes:
        old_timeout = self._serial.timeout
        self._serial.timeout = timeout
        try:
            self._serial.reset_input_buffer()
            self._send_command(cmd, payload)
            descriptor, payload_len = self._read_descriptor()
            if descriptor[6] != expected_ans_type:
                raise LidarError(
                    f"Unexpected ans type {descriptor[6]:#x}, "
                    f"wanted {expected_ans_type:#x}"
                )
            if payload_len == 0:
                return b""
            resp = self._read_exact(payload_len)
            if cmd == _CMD_GET_LIDAR_CONF and len(resp) >= 8:
                return resp[8:]
            return resp
        finally:
            self._serial.timeout = old_timeout

    def get_typical_scan_mode(self) -> Optional[int]:
        try:
            req = struct.pack("<I", _CONF_SCAN_MODE_TYPICAL)
            data = self._command_with_response(
                _CMD_GET_LIDAR_CONF, req, _ANS_TYPE_GET_LIDAR_CONF
            )
            if len(data) >= 2:
                return struct.unpack("<H", data[0:2])[0]
        except LidarError:
            pass
        return None

    def get_scan_mode_ans_type(self, mode_id: int) -> Optional[int]:
        try:
            req = struct.pack("<I", _CONF_SCAN_MODE_ANS_TYPE) + struct.pack(
                "<H", mode_id
            )
            data = self._command_with_response(
                _CMD_GET_LIDAR_CONF, req, _ANS_TYPE_GET_LIDAR_CONF
            )
            if data:
                return data[0]
        except LidarError:
            pass
        return None

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

    def _start_legacy_scan(self) -> None:
        """Legacy SCAN (0x20): command first, motor second (S2 USB proven order)."""
        self._send_command(_CMD_SCAN)
        try:
            header = self._serial.read(2)
            if header == bytes([_SYNC_BYTE, 0x5A]):
                self._serial.read(5)
        except Exception:
            pass
        self.start_motor()
        time.sleep(0.5)

    def _start_express_scan(self, mode_id: int, *, hq: bool = False) -> int:
        payload = struct.pack("<BI", mode_id & 0xFF, 0)
        cmd = _CMD_HQ_SCAN if hq else _CMD_EXPRESS_SCAN
        self._send_command(cmd, payload)
        descriptor, plen = self._read_descriptor()
        if plen:
            self._serial.read(plen)
        return descriptor[6]

    def start_scan(self, mode: str = "auto") -> None:
        """Enter scan mode once and spin the motor (keep streaming)."""
        self.stop()
        time.sleep(0.1)
        self.reset()
        self.stop()
        time.sleep(0.1)
        self._serial.reset_input_buffer()

        mode_key = mode.lower().strip()
        scan_format = "legacy"
        express_mode_id = 0

        if mode_key == "hq":
            typical = self.get_typical_scan_mode()
            express_mode_id = typical if typical is not None else 2
            scan_format = "hq"
        elif mode_key in ("auto", "express"):
            typical = self.get_typical_scan_mode()
            if typical is not None:
                express_mode_id = typical
                ans_type = self.get_scan_mode_ans_type(typical)
                if ans_type is None:
                    scan_format = "legacy"
                else:
                    scan_format = _ans_type_to_format(ans_type)
            elif mode_key == "express":
                scan_format = "dense"
                express_mode_id = 2
            else:
                scan_format = "legacy"
        elif mode_key == "legacy":
            scan_format = "legacy"
        else:
            scan_format = "legacy"

        if scan_format in ("ultra_dense", "hq"):
            scan_format = "legacy"
            express_mode_id = 0

        try:
            if scan_format == "legacy":
                self._start_legacy_scan()
            else:
                self.start_motor()
                time.sleep(0.1)
                if scan_format == "hq":
                    ans_type = self._start_express_scan(express_mode_id, hq=True)
                    scan_format = _ans_type_to_format(ans_type)
                else:
                    ans_type = self._start_express_scan(express_mode_id, hq=False)
                    if ans_type == _ANS_TYPE_MEASUREMENT:
                        self.stop()
                        time.sleep(0.1)
                        self._serial.reset_input_buffer()
                        self._start_legacy_scan()
                        scan_format = "legacy"
                    else:
                        scan_format = _ans_type_to_format(ans_type)
                time.sleep(0.4)
        except Exception:
            scan_format = "legacy"
            express_mode_id = 0
            self._serial.reset_input_buffer()
            self._start_legacy_scan()

        if scan_format in ("ultra_dense", "hq"):
            self.stop()
            time.sleep(0.1)
            self._serial.reset_input_buffer()
            self._start_legacy_scan()
            scan_format = "legacy"
            express_mode_id = 0

        self._scan_format = scan_format
        self._express_mode_id = express_mode_id
        self._decoder = _make_scan_decoder(scan_format)
        self._decoder.reset()

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

        if self._scan_format == "legacy":
            while time.monotonic() < deadline and len(points) < max_points:
                if stop_event is not None and stop_event.is_set():
                    break
                if len(raw_buf) < _MEASUREMENT_LEN:
                    chunk_in = self._serial.read(512)
                    if chunk_in:
                        raw_buf.extend(chunk_in)
                    elif not raw_buf:
                        continue
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
                    chunk_in = self._serial.read(512)
                    if chunk_in:
                        raw_buf.extend(chunk_in)
        else:
            decoder = self._decoder
            while time.monotonic() < deadline and len(points) < max_points:
                if stop_event is not None and stop_event.is_set():
                    break
                chunk_in = self._serial.read(512)
                if not chunk_in:
                    continue
                new_pts = decoder.feed(chunk_in)
                if new_pts:
                    points.extend(new_pts)
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
        scan_mode: str = "auto",
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
        block_confirm_polls: int = 2,
    ) -> None:
        self._port_configured = port
        self._symlink = symlink
        self._usb_serial = usb_serial
        self._baud = baud
        self._scan_mode = scan_mode
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
        self._block_confirm_polls = max(1, block_confirm_polls)
        self._debouncer = BubbleDebouncer(confirm_polls=self._block_confirm_polls)

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
            f"scan_mode={self._scan_mode} "
            f"poll_hz={1.0/self._poll_interval:.1f} "
            f"sample={self._sample_sec:.2f}s "
            f"block_confirm={self._block_confirm_polls})")

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
                driver.start_scan(mode=self._scan_mode)
                log("lidar", f"active scan: {driver.scan_mode_label}")
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
                            driver.start_scan(mode=self._scan_mode)
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
        per_sector_blocked: dict = {}
        per_sector_raw: dict = {}
        any_blocked = False

        for name, (min_d, max_d, bubble) in self._sectors.items():
            d = _min_distance_in_sector(
                points, min_d, max_d,
                self._range_min, self._range_max, self._min_quality,
            )
            dists[f"lidar_{name}_m"] = None if d == float("inf") else round(d, 3)
            raw_blocked = (d != float("inf")) and (d < bubble)
            per_sector_raw[f"lidar_blocked_{name}_raw"] = raw_blocked
            debounced = self._debouncer.update(name, raw_blocked)
            per_sector_blocked[f"lidar_blocked_{name}"] = debounced
            any_blocked = any_blocked or debounced

        upd = {
            "lidar_status": "ok",
            "lidar_ts_mono": now_mono(),
            "lidar_point_count": len(points),
            "lidar_blocked": any_blocked,
        }
        upd.update(dists)
        upd.update(per_sector_raw)
        upd.update(per_sector_blocked)

        with self._lock:
            self._data.update(upd)