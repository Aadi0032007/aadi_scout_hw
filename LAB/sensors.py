# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Sensor readers — IMU (WIT/JY901 over UART), GPS (NMEA over UDP),
TEMPerHUM (USB HID), Battery (Segway BMS via Docker ROS1).

Battery reader uses ONE persistent `docker exec rostopic echo` subprocess
and parses its streaming YAML output. This replaces the previous
poll-and-timeout design, which was paying the ROS-source + rostopic-startup
cost per poll and timing out whenever `/bms_fb` published slower than 3 s.
"""

import errno
import glob
import math
import os
import pathlib
import re
import struct
import subprocess
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

# Re-export LidarReader so callers can stay symmetric with ImuReader/GpsReader.
# The implementation lives in lidar.py because the Slamtec protocol driver is
# substantial enough to deserve its own module.
from .lidar import LidarReader  # noqa: E402,F401


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

# ═══ TEMPerHUM (PCsensor USB HID) ═════════════════════════════════════════════
#
# Reads temperature and humidity from a PCsensor TEMPerHUM (VID:PID 3553:A001
# by default) via raw hidraw. Discovery is by VID/PID in sysfs so it survives
# /dev/hidrawN renumbering across reboots / replugs.

_THUM_QUERY = bytes([0x01, 0x80, 0x33, 0x01, 0x00, 0x00, 0x00, 0x00])
_THUM_HID_ID_RE = re.compile(
    r"^HID_ID=[0-9A-Fa-f]+:0*([0-9A-Fa-f]+):0*([0-9A-Fa-f]+)", re.M
)

# Plausibility bounds — anything outside means wrong interface or garbage frame.
_THUM_TEMP_C_MIN, _THUM_TEMP_C_MAX = -40.0, 125.0
_THUM_RH_MIN,     _THUM_RH_MAX     = 0.0, 100.0


def _thum_candidates(vid: str, pid: str):
    """Yield (hidraw_path, interface_number) for every hidraw matching VID:PID."""
    want = (vid.upper(), pid.upper())
    for hr in sorted(glob.glob("/dev/hidraw*")):
        name = hr.rsplit("/", 1)[1]
        sysdev = pathlib.Path(f"/sys/class/hidraw/{name}/device")
        try:
            uevent = (sysdev / "uevent").read_text()
        except OSError:
            continue
        m = _THUM_HID_ID_RE.search(uevent)
        if not m:
            continue
        if (m.group(1).upper(), m.group(2).upper()) != want:
            continue
        # Parent dir is the USB interface, e.g. "1-2.4.3.1:1.1" — trailing ".N"
        # after the colon is bInterfaceNumber.
        try:
            iface_dir = sysdev.resolve().parent.name
            iface_num = int(iface_dir.rsplit(".", 1)[1])
        except (ValueError, IndexError):
            iface_num = -1
        yield hr, iface_num


def _thum_try_read(dev_path: str, timeout: float = 0.3):
    """Send query, read 8 bytes, return (temp_c, rh) or None."""
    try:
        fd = os.open(dev_path, os.O_RDWR | os.O_NONBLOCK)
    except OSError as e:
        if e.errno in (errno.EACCES, errno.EPERM):
            raise PermissionError(f"no permission on {dev_path}") from e
        return None
    try:
        try:
            os.write(fd, _THUM_QUERY)
        except OSError:
            return None
        deadline = time.monotonic() + timeout
        buf = b""
        while time.monotonic() < deadline and len(buf) < 8:
            try:
                chunk = os.read(fd, 8 - len(buf))
                if chunk:
                    buf += chunk
            except BlockingIOError:
                time.sleep(0.01)
        if len(buf) < 8:
            return None
        temp_c = struct.unpack(">h", buf[2:4])[0] / 100.0
        rh     = struct.unpack(">H", buf[4:6])[0] / 100.0
        if not (_THUM_TEMP_C_MIN <= temp_c <= _THUM_TEMP_C_MAX): return None
        if not (_THUM_RH_MIN     <= rh     <= _THUM_RH_MAX):     return None
        return temp_c, rh
    finally:
        os.close(fd)


class TempHumReader:
    """
    Reads temperature (°F) and humidity (%) from a PCsensor TEMPerHUM via hidraw.

    Discovers the sensor by VID:PID at startup and re-discovers automatically
    if the device disappears (replug). Exposes a snapshot via get() returning
    {"temp_f": float, "humidity_pct": float, "age_sec": float} — same get()
    pattern as ImuReader / GpsReader.
    """

    def __init__(
        self,
        vid: str = "3553",
        pid: str = "A001",
        poll_sec: float = 2.0,
    ) -> None:
        self._vid = vid
        self._pid = pid
        self._poll_sec = max(0.5, float(poll_sec))
        self._dev_path: Optional[str] = None
        self._data: dict = {}
        self._last_update: float = 0.0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="temphum-reader")

    def start(self) -> None:
        self._thread.start()
        log("sensors", f"TempHum reader started (VID:PID {self._vid}:{self._pid})")

    def get(self) -> dict:
        with self._lock:
            d = dict(self._data)
        if self._last_update > 0:
            d["age_sec"] = time.monotonic() - self._last_update
        return d

    def stop(self) -> None:
        self._stop.set()

    # ── internals ─────────────────────────────────────────────────────────────

    def _discover(self) -> Optional[str]:
        """Find the sensor interface (interface 1 preferred) and validate it reads."""
        candidates = list(_thum_candidates(self._vid, self._pid))
        if not candidates:
            return None
        # Interface 1 holds the sensor on this firmware; try it first.
        candidates.sort(key=lambda c: (c[1] != 1, c[1]))
        for path, _iface in candidates:
            try:
                if _thum_try_read(path) is not None:
                    return path
            except PermissionError as e:
                log("sensors", f"TempHum: {e} (add udev rule or run as root)")
                return None
        return None

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            if self._dev_path is None:
                self._dev_path = self._discover()
                if self._dev_path is None:
                    log("sensors",
                        f"TempHum: no device {self._vid}:{self._pid} — retrying")
                    self._stop.wait(timeout=backoff)
                    backoff = min(backoff * 2, 30.0)
                    continue
                log("sensors", f"TempHum: using {self._dev_path}")
                backoff = 1.0

            try:
                result = _thum_try_read(self._dev_path)
            except PermissionError:
                self._dev_path = None
                self._stop.wait(timeout=5.0)
                continue
            except OSError:
                result = None

            if result is None:
                # Probably unplugged; force rediscovery.
                self._dev_path = None
                self._stop.wait(timeout=2.0)
                continue

            temp_c, rh = result
            with self._lock:
                self._data["temp_f"]       = temp_c * 9.0 / 5.0 + 32.0
                self._data["temp_c"]       = temp_c
                self._data["humidity_pct"] = rh
                self._last_update = time.monotonic()

            self._stop.wait(timeout=self._poll_sec)


# ═══ Battery (Segway BMS via Docker ROS1, streaming) ══════════════════════════
#
# Previous design shelled out to `rostopic echo -n 1 /bms_fb` every 2 s. That
# waited for the *next* message and paid the ROS-source + rostopic-startup
# cost per poll — which is exactly why the 3 s timeouts fired constantly.
#
# New design: one persistent `docker exec rostopic echo` subprocess streams
# messages continuously. Each YAML record (separated by `---`) is parsed and
# used to update the snapshot dict. `stdbuf -oL` inside the container forces
# line-buffered stdout so we see records as they arrive rather than in 4 KB
# chunks. Log spam is throttled to one line on stream start and one on stream
# loss / respawn.
#
# Fields exposed via get():
#   bat_soc       - state of charge, %
#   bat_charging  - True if charging, False if discharging
#   bat_vol       - voltage, mV
#   bat_current   - current, mA
#   bat_temp      - temperature, °C
#   age_sec       - seconds since the last successful read

_BMS_FIELD_RE = re.compile(
    r"^\s*(bat_soc|bat_charging|bat_vol|bat_current|bat_temp)\s*:\s*(-?[0-9.]+)\s*$"
)


class BatteryReader:
    """Reads Segway BMS state from /bms_fb inside the segway_ros1 container.

    Runs ONE persistent `docker exec ... rostopic echo` subprocess and parses
    its streaming output. This avoids the fresh-exec overhead per poll (which
    caused the 3 s timeouts in the previous design — most of the budget was
    spent sourcing ROS and starting rostopic, not waiting for a message).

    Snapshot dict keys are unchanged: bat_soc, bat_charging, bat_vol,
    bat_current, bat_temp, age_sec.

    If the subprocess dies or the topic goes silent for `stale_after_sec`,
    the reader tears it down and respawns with exponential backoff.
    """

    def __init__(
        self,
        container:       str   = "segway_ros1",
        topic:           str   = "/bms_fb",
        ros_setup:       str   = "/opt/ros/noetic/setup.bash",
        ws_setup:        str   = "/root/catkin_ws/devel/setup.bash",
        stale_after_sec: float = 30.0,
        # kept for back-compat with existing callers — no-ops in this design.
        poll_sec:        float = 2.0,
        cmd_timeout:     float = 3.0,
    ) -> None:
        self._container       = container
        self._topic           = topic
        self._ros_setup       = ros_setup
        self._ws_setup        = ws_setup
        self._stale_after_sec = stale_after_sec

        self._data: dict = {}
        self._last_msg_t: float = 0.0
        self._lock   = threading.Lock()
        self._stop   = threading.Event()
        self._proc:  Optional[subprocess.Popen] = None
        self._thread = threading.Thread(target=self._run, daemon=True, name="battery-reader")

    def start(self) -> None:
        self._thread.start()
        log("sensors",
            f"Battery reader started (container={self._container}, topic={self._topic})")

    def get(self) -> dict:
        with self._lock:
            d = dict(self._data)
        if self._last_msg_t > 0:
            d["age_sec"] = time.monotonic() - self._last_msg_t
        return d

    def stop(self) -> None:
        self._stop.set()
        self._kill_proc()

    # ── internals ─────────────────────────────────────────────────────────

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            proc = self._spawn()
            if proc is None:
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 30.0)
                continue

            self._proc = proc
            log("sensors", "Battery: stream started")
            backoff = 1.0

            # One YAML "record" per message. rostopic separates them with "---".
            buf: list = []
            stream_alive = True
            last_stale_check = time.monotonic()

            try:
                assert proc.stdout is not None
                for raw in proc.stdout:
                    if self._stop.is_set():
                        break
                    line = raw.rstrip("\n")
                    if line.strip() == "---":
                        self._flush_record(buf)
                        buf.clear()
                    else:
                        buf.append(line)

                    # Cheap stale check between messages — if the topic dies
                    # mid-stream, we notice within stale_after_sec instead of
                    # blocking on readline forever.
                    now = time.monotonic()
                    if now - last_stale_check > 5.0:
                        last_stale_check = now
                        if (self._last_msg_t > 0
                                and (now - self._last_msg_t) > self._stale_after_sec):
                            log("sensors",
                                f"Battery: no message for "
                                f"{now - self._last_msg_t:.0f}s — respawning")
                            stream_alive = False
                            break
            except Exception as exc:
                log("sensors", f"Battery: stream read error: {exc}")
                stream_alive = False

            if stream_alive and not self._stop.is_set():
                log("sensors", "Battery: stream ended — respawning")

            self._kill_proc()

    def _spawn(self) -> Optional[subprocess.Popen]:
        """Start a persistent `docker exec rostopic echo` subprocess.

        `stdbuf -oL` forces rostopic to line-buffer inside the container so we
        get messages as they arrive, not in 4 KB chunks.
        """
        cmd = [
            "docker", "exec", "-i", self._container, "bash", "-c",
            f"source {self._ros_setup} && source {self._ws_setup} && "
            f"exec stdbuf -oL rostopic echo {self._topic}",
        ]
        try:
            return subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,   # line-buffered on the Python side
            )
        except Exception as exc:
            log("sensors", f"Battery: docker exec failed: {exc}")
            return None

    def _kill_proc(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is None:
            return
        try:
            proc.terminate()
            try:
                proc.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                proc.kill()
        except Exception:
            pass

    def _flush_record(self, lines: list) -> None:
        """Parse one YAML record (a full message between `---` separators)."""
        upd: dict = {}
        for line in lines:
            m = _BMS_FIELD_RE.match(line)
            if not m:
                continue
            field, raw = m.group(1), m.group(2)
            try:
                value = float(raw)
            except ValueError:
                continue

            if field == "bat_charging":
                upd["bat_charging"] = value > 0.5
            elif field == "bat_soc":
                upd["bat_soc"] = value
            elif field == "bat_vol":
                upd["bat_vol"] = value
            elif field == "bat_current":
                upd["bat_current"] = value
            elif field == "bat_temp":
                upd["bat_temp"] = value

        if not upd:
            return
        with self._lock:
            self._data.update(upd)
            self._last_msg_t = time.monotonic()