#!/usr/bin/env python3
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations

"""
GPS mux — single owner of /dev/ttyCH341USB2.

Exposes two consumers from one physical port:
    1. PTY symlinked at /tmp/scoutlab_gps_pty  (bidirectional — Polaris talks here)
    2. UDP socket 127.0.0.1:57002              (NMEA fan-out — teleop GpsReader)

NMEA from the receiver goes to BOTH the PTY (so Polaris can compute a position
and request corrections for it) and the UDP socket (so teleop gets the full
NMEA stream including HDT and #ADRNAVA).

RTCM corrections written by Polaris come back in via the PTY and are forwarded
to the real receiver.

Reconnects on USB drop. PTY symlink stays valid across reconnects.

Env overrides:
    GPS_REAL_PORT  (default /dev/ttyCH341USB2)
    GPS_REAL_BAUD  (default 115200)
    GPS_PTY_PATH   (default /tmp/scoutlab_gps_pty)
    GPS_UDP_HOST   (default 127.0.0.1)
    GPS_UDP_PORT   (default 57002)
"""

import errno
import fcntl
import os
import pty
import select
import signal
import socket
import sys
import termios
import time
import tty

import serial


REAL_PORT   = os.environ.get("GPS_REAL_PORT", "/dev/ttyCH341USB2")
REAL_BAUD   = int(os.environ.get("GPS_REAL_BAUD", "115200"))
PTY_SYMLINK = os.environ.get("GPS_PTY_PATH",   "/tmp/scoutlab_gps_pty")
UDP_HOST    = os.environ.get("GPS_UDP_HOST",   "127.0.0.1")
UDP_PORT    = int(os.environ.get("GPS_UDP_PORT", "57002"))


_running = True
def _on_signal(*_):
    global _running
    _running = False


def _open_real():
    try:
        ser = serial.Serial(
            port=REAL_PORT, baudrate=REAL_BAUD,
            bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=0, write_timeout=1.0,
        )
        # Avoid toggling DTR/RTS on open — some receivers interpret as reset
        try: ser.dtr = False
        except Exception: pass
        try: ser.rts = False
        except Exception: pass
        return ser
    except Exception as exc:
        print(f"[gps_mux] open {REAL_PORT} failed: {exc}", flush=True)
        return None


def _create_pty():
    master_fd, slave_fd = pty.openpty()
    slave_name = os.ttyname(slave_fd)
    # Raw mode on slave (Polaris sees a clean byte stream)
    tty.setraw(slave_fd, termios.TCSANOW)
    # Non-blocking master so our select loop never wedges
    flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    # Stable symlink for downstream tools
    try:
        if os.path.islink(PTY_SYMLINK) or os.path.exists(PTY_SYMLINK):
            os.unlink(PTY_SYMLINK)
    except OSError:
        pass
    os.symlink(slave_name, PTY_SYMLINK)
    return master_fd, slave_fd, slave_name


def main():
    signal.signal(signal.SIGINT,  _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    master_fd, slave_fd, slave_name = _create_pty()
    print(f"[gps_mux] PTY {slave_name} → {PTY_SYMLINK}", flush=True)

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    print(f"[gps_mux] NMEA fan-out → udp://{UDP_HOST}:{UDP_PORT}", flush=True)
    print(f"[gps_mux] real port    = {REAL_PORT} @ {REAL_BAUD}", flush=True)

    ser = None
    backoff = 1.0
    last_attempt = 0.0
    nmea_buf = bytearray()

    try:
        while _running:
            now = time.monotonic()

            if ser is None:
                if (now - last_attempt) >= backoff:
                    last_attempt = now
                    ser = _open_real()
                    if ser is None:
                        backoff = min(backoff * 2, 10.0)
                        print(f"[gps_mux] retry {REAL_PORT} in {backoff:.1f}s", flush=True)
                    else:
                        backoff = 1.0
                        print(f"[gps_mux] opened {REAL_PORT}", flush=True)
                else:
                    time.sleep(0.1)
                    continue

            try:
                rlist, _, _ = select.select([ser.fileno(), master_fd], [], [], 0.5)
            except (OSError, ValueError):
                try: ser.close()
                except Exception: pass
                ser = None
                continue

            # Receiver → Polaris (PTY) + UDP fan-out
            if ser is not None and ser.fileno() in rlist:
                try:
                    data = os.read(ser.fileno(), 4096)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                        data = b""
                    else:
                        print(f"[gps_mux] read err: {exc}", flush=True)
                        try: ser.close()
                        except Exception: pass
                        ser = None
                        data = b""

                if data:
                    try: os.write(master_fd, data)
                    except OSError: pass

                    nmea_buf.extend(data)
                    while True:
                        idx = nmea_buf.find(b"\n")
                        if idx < 0:
                            break
                        line = bytes(nmea_buf[:idx + 1])
                        del nmea_buf[:idx + 1]
                        text = line.replace(b"\r", b"").strip()
                        if text:
                            try: udp.sendto(text + b"\n", (UDP_HOST, UDP_PORT))
                            except OSError: pass
                    if len(nmea_buf) > 8192:
                        del nmea_buf[:4096]

            # Polaris (PTY) → receiver  (RTCM corrections)
            if master_fd in rlist:
                try:
                    data = os.read(master_fd, 4096)
                except OSError as exc:
                    if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK, errno.EIO):
                        data = b""
                    else:
                        data = b""

                if data and ser is not None:
                    try:
                        ser.write(data)
                    except (serial.SerialTimeoutException, serial.SerialException, OSError):
                        # Heavy data pressure or full hardware buffers.
                        # Simply drop this packet and keep the serial line alive!
                        pass
                    except Exception as exc:
                        # Actual device unplugged or unrecoverable system failure.
                        print(f"[gps_mux] Fatal hardware failure: {exc}", flush=True)
                        try: ser.close()
                        except Exception: pass
                        ser = None
    finally:
        print("[gps_mux] shutdown", flush=True)
        if ser is not None:
            try: ser.close()
            except Exception: pass
        try: os.close(master_fd)
        except OSError: pass
        try: os.close(slave_fd)
        except OSError: pass
        try:
            if os.path.islink(PTY_SYMLINK):
                os.unlink(PTY_SYMLINK)
        except OSError:
            pass
        try: udp.close()
        except Exception: pass


if __name__ == "__main__":
    main()
