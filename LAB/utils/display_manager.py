#!/usr/bin/env python3
"""
DisplayManager — HDMI still-image display for Revobots (MPV JSON IPC)

Vendored from Revobots/development/revo_display_manager.py for teleop.
"""

from __future__ import annotations

import json
import logging
import os
import re
import socket
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_IPC_SOCKET = "/tmp/revo-display.sock"
DEFAULT_DISPLAY = ":0"
SOCKET_WAIT_TIMEOUT_S = 8.0
SOCKET_POLL_S = 0.05
IPC_RECV_TIMEOUT_S = 2.0

_ROTATION_SUFFIX_RE = re.compile(
    r"_(?P<dir>AC|C)(?P<deg>\d+)R$",
    re.IGNORECASE,
)


def parse_rotation_from_filename(path: str | Path) -> int:
    """Return MPV video-rotate degrees (clockwise, 0-359) from filename suffix."""
    stem = Path(path).stem
    match = _ROTATION_SUFFIX_RE.search(stem)
    if not match:
        return 0
    degrees = int(match.group("deg")) % 360
    direction = match.group("dir").upper()
    if direction == "AC":
        degrees = (360 - degrees) % 360
    return degrees


class DisplayManager:
    """Thread-safe HDMI still-image display backed by a single MPV process."""

    def __init__(
        self,
        ipc_socket: str = DEFAULT_IPC_SOCKET,
        display: str = DEFAULT_DISPLAY,
        mpv_bin: str = "mpv",
    ) -> None:
        self.ipc_socket = ipc_socket
        self.display = display or DEFAULT_DISPLAY
        self.mpv_bin = mpv_bin
        self._proc: subprocess.Popen | None = None
        self._lock = threading.RLock()

    def is_running(self) -> bool:
        with self._lock:
            return self._process_alive() and self._socket_ready()

    def start(self) -> bool:
        with self._lock:
            try:
                return self._ensure_mpv()
            except Exception:
                logger.exception("DisplayManager.start failed")
                return False

    def stop(self) -> None:
        with self._lock:
            try:
                self._stop_mpv()
            except Exception:
                logger.exception("DisplayManager.stop failed")

    def show_image(self, path: str | Path, rotate: int | None = None) -> bool:
        """
        Show an image fullscreen via MPV loadfile/replace.

        Rotation comes from the optional rotate argument, else from filename
        suffix (*_C90R.png, etc.). Returns True on success.
        """
        with self._lock:
            try:
                abs_path = self._validate_image(path)
                if abs_path is None:
                    return False
                if rotate is None:
                    rotate = parse_rotation_from_filename(abs_path)
                return self._loadfile_with_retry(abs_path, rotate=rotate)
            except Exception:
                logger.exception("DisplayManager.show_image failed for %s", path)
                return False

    def clear(self) -> bool:
        with self._lock:
            try:
                if not self._ensure_mpv():
                    return False
                ok = self._send_command(["stop"])
                if not ok:
                    if self._ensure_mpv(force_restart=True):
                        ok = self._send_command(["stop"])
                return bool(ok)
            except Exception:
                logger.exception("DisplayManager.clear failed")
                return False

    def _validate_image(self, path: str | Path) -> str | None:
        abs_path = os.path.abspath(os.path.expanduser(str(path)))
        if not os.path.isfile(abs_path):
            logger.error("Image not found: %s", abs_path)
            return None
        return abs_path

    def _process_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def _socket_ready(self) -> bool:
        path = self.ipc_socket
        if not os.path.exists(path):
            return False
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.2)
                sock.connect(path)
            return True
        except OSError:
            return False

    def _cleanup_stale_socket(self) -> None:
        path = self.ipc_socket
        if not os.path.exists(path):
            return
        if self._socket_ready():
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.5)
                    sock.connect(path)
                    sock.sendall(b'{"command":["quit"]}\n')
            except OSError:
                pass
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and os.path.exists(path):
                time.sleep(0.05)
        if os.path.exists(path):
            try:
                os.unlink(path)
                logger.info("Removed IPC socket %s before start", path)
            except OSError as exc:
                logger.warning("Could not remove socket %s: %s", path, exc)

    def _mpv_cmd(self) -> list[str]:
        return [
            self.mpv_bin,
            "--fullscreen",
            "--no-border",
            "--image-display-duration=inf",
            "--idle=yes",
            "--keep-open=yes",
            "--really-quiet",
            f"--input-ipc-server={self.ipc_socket}",
            "--force-window=immediate",
            "--no-osc",
            "--osd-level=0",
            "--cursor-autohide=always",
        ]

    def _stop_mpv(self) -> None:
        proc = self._proc
        self._proc = None
        if proc is not None and proc.poll() is None:
            try:
                self._send_command(["quit"], ensure_running=False)
            except Exception:
                pass
            try:
                proc.terminate()
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=1.0)
                except Exception:
                    pass
        if os.path.exists(self.ipc_socket):
            try:
                os.unlink(self.ipc_socket)
            except OSError:
                pass

    def _start_mpv(self) -> bool:
        self._cleanup_stale_socket()
        if self._process_alive():
            try:
                self._proc.terminate()
                self._proc.wait(timeout=2.0)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None

        env = os.environ.copy()
        env["DISPLAY"] = self.display
        if "XAUTHORITY" not in env or self.display in (":0", ":0.0"):
            for cand in (
                f"/run/user/{os.getuid()}/gdm/Xauthority",
                os.path.expanduser("~/.Xauthority"),
            ):
                if os.path.isfile(cand):
                    env["XAUTHORITY"] = cand
                    break

        err_path = f"{self.ipc_socket}.stderr"
        try:
            err_f = open(err_path, "wb")
        except OSError:
            err_f = subprocess.DEVNULL

        try:
            self._proc = subprocess.Popen(
                self._mpv_cmd(),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=err_f,
                shell=False,
            )
        except FileNotFoundError:
            if err_f is not subprocess.DEVNULL:
                err_f.close()
            logger.error(
                "mpv not found (%s). Install with: sudo apt install mpv",
                self.mpv_bin,
            )
            self._proc = None
            return False
        except Exception:
            if err_f is not subprocess.DEVNULL:
                err_f.close()
            logger.exception("Failed to launch mpv")
            self._proc = None
            return False
        finally:
            if err_f is not subprocess.DEVNULL:
                try:
                    err_f.close()
                except Exception:
                    pass

        if not self._wait_for_socket(SOCKET_WAIT_TIMEOUT_S):
            stderr = ""
            try:
                with open(err_path, "r", errors="replace") as fh:
                    stderr = fh.read()[-800:]
            except OSError:
                pass
            logger.error(
                "MPV IPC socket %s did not appear (DISPLAY=%s XAUTHORITY=%s). %s",
                self.ipc_socket,
                env.get("DISPLAY"),
                env.get("XAUTHORITY", ""),
                stderr.strip() or "(no stderr)",
            )
            self._stop_mpv()
            return False

        logger.info(
            "MPV started pid=%s DISPLAY=%s ipc=%s",
            self._proc.pid if self._proc else "?",
            self.display,
            self.ipc_socket,
        )
        return True

    def _wait_for_socket(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if not self._process_alive():
                return False
            if self._socket_ready():
                return True
            time.sleep(SOCKET_POLL_S)
        return self._socket_ready()

    def _ensure_mpv(self, force_restart: bool = False) -> bool:
        if force_restart:
            self._stop_mpv()
            return self._start_mpv()
        if self._process_alive() and self._socket_ready():
            return True
        if self._socket_ready() and not self._process_alive():
            logger.info("Reusing existing MPV IPC at %s", self.ipc_socket)
            return True
        return self._start_mpv()

    def _loadfile_with_retry(self, abs_path: str, rotate: int = 0) -> bool:
        if not self._ensure_mpv():
            return False
        if self._show_with_rotation(abs_path, rotate):
            return True
        logger.warning("MPV IPC failed; restarting once and retrying loadfile")
        if not self._ensure_mpv(force_restart=True):
            return False
        return self._show_with_rotation(abs_path, rotate)

    def _show_with_rotation(self, abs_path: str, rotate: int) -> bool:
        if not self._send_command(["loadfile", abs_path, "replace"]):
            return False
        if not self._send_command(["set_property", "video-rotate", int(rotate) % 360]):
            logger.error("Failed to set video-rotate=%s for %s", rotate, abs_path)
            return False
        if rotate:
            logger.info("Showing %s with video-rotate=%s", abs_path, rotate)
        return True

    def _send_command(
        self,
        command: list[Any],
        ensure_running: bool = True,
    ) -> dict[str, Any] | None:
        if ensure_running and not self._socket_ready():
            if not self._ensure_mpv():
                return None
        if not self._socket_ready():
            if not self._wait_for_socket(SOCKET_WAIT_TIMEOUT_S if ensure_running else 0.2):
                if ensure_running:
                    logger.error("IPC socket missing: %s", self.ipc_socket)
                return None

        payload = (json.dumps({"command": command}) + "\n").encode("utf-8")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(IPC_RECV_TIMEOUT_S)
                sock.connect(self.ipc_socket)
                sock.sendall(payload)
                raw = self._recv_line(sock)
        except OSError as exc:
            logger.error("MPV IPC send failed (%s): %s", command[0], exc)
            return None

        if not raw:
            logger.error("Empty IPC response for command %s", command[0])
            return None

        try:
            response = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("Bad IPC JSON: %r", raw[:200])
            return None

        if response.get("error") not in (None, "success"):
            logger.error("MPV command error %s: %s", command, response.get("error"))
            return None
        return response

    @staticmethod
    def _recv_line(sock: socket.socket) -> str:
        chunks: list[bytes] = []
        while True:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                break
            if not data:
                break
            chunks.append(data)
            if b"\n" in data:
                break
        return b"".join(chunks).split(b"\n", 1)[0].decode("utf-8", errors="replace")


def resolve_display_image(name: str, display_dir: str | Path | None = None) -> str:
    """Resolve a filename or path under the Revobots display directory."""
    if display_dir is None:
        display_dir = Path.home() / "Revobots" / "display"
    display_dir = Path(display_dir)
    candidate = Path(name)
    if not candidate.is_absolute():
        candidate = display_dir / name
    return str(candidate.resolve())