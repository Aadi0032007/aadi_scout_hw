# -*- coding: utf-8 -*-
"""
display.py — MPV fullscreen display subsystem (HDMI still images).

Runtime-facing API:
    start()                 — start MPV and show the default wallpaper
    stop()                  — quit MPV and release the display
    show_text(text)         — render text to a temp PNG and show fullscreen
    set_wallpaper(image)    — show a PNG/JPG fullscreen, resolved from display/
    clear()                 — return to the default wallpaper

Uses DisplayManager (mpv JSON IPC) so pygame remains free for the local
gamepad joystick subsystem.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Optional

from .common import log
from .utils.display_manager import (
    DisplayManager,
    parse_rotation_from_filename,
)

_TEXT_IMAGE_PATH = "/tmp/revo-display-text.png"


class DisplayController:
    def __init__(
        self,
        display: Optional[str] = None,
        asset_dir: str = "",
        default_wallpaper: str = "REVOBOTS_LOGO_AC90R.png",
        rotate: int = 90,
        fullscreen: bool = True,
        fps: int = 30,
        enabled: bool = True,
    ) -> None:
        self._display = display or _resolve_display_name()
        self._asset_dir = (
            Path(asset_dir).expanduser()
            if asset_dir
            else Path.home() / "Revobots" / "display"
        )
        self._default_wallpaper = default_wallpaper
        self._rotate = rotate if rotate in (0, 90, 180, 270) else 0
        self._enabled = bool(enabled)
        # fullscreen/fps kept for config API compatibility; MPV owns the window.
        _ = fullscreen, fps

        self._queue: Queue[dict] = Queue(maxsize=8)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._mgr: Optional[DisplayManager] = None

    def start(self) -> None:
        if not self._enabled:
            log("display", "disabled by config")
            return
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="display"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._enqueue({"cmd": "stop"})
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def show_text(self, text: str) -> None:
        text = str(text or "").strip()
        if not text:
            return
        self._enqueue({"cmd": "text", "text": text})

    def set_wallpaper(self, image: str) -> None:
        image = str(image or "").strip()
        if not image:
            return
        self._enqueue({"cmd": "wallpaper", "image": image})

    def clear(self) -> None:
        self._enqueue({"cmd": "clear"})

    def _enqueue(self, item: dict) -> None:
        if not self._enabled:
            return
        try:
            self._queue.put_nowait(item)
        except Full:
            try:
                self._queue.get_nowait()
            except Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except Full:
                pass

    def _run(self) -> None:
        try:
            self._mgr = DisplayManager(display=self._display)
            if not self._mgr.start():
                log("display", "mpv start failed — is mpv installed? (sudo apt install mpv)")
                return

            log("display", f"ready DISPLAY={self._display} asset_dir={self._asset_dir}")
            if self._default_wallpaper:
                self._show_wallpaper(self._default_wallpaper)

            while not self._stop.is_set():
                try:
                    item = self._queue.get(timeout=0.25)
                except Empty:
                    continue

                cmd = item.get("cmd")
                if cmd == "stop":
                    break
                if cmd == "text":
                    self._show_text(str(item.get("text") or ""))
                elif cmd == "wallpaper":
                    self._show_wallpaper(str(item.get("image") or ""))
                elif cmd == "clear":
                    if self._default_wallpaper:
                        self._show_wallpaper(self._default_wallpaper)
                    elif self._mgr is not None:
                        self._mgr.clear()
        except Exception as exc:
            log("display", f"display worker error: {exc}")
        finally:
            if self._mgr is not None:
                try:
                    self._mgr.stop()
                except Exception:
                    pass
                self._mgr = None
            log("display", "stopped")

    def _show_wallpaper(self, raw: str) -> None:
        if self._mgr is None:
            return
        path = self._resolve_image_path(raw)
        if path is None:
            log("display", f"wallpaper not found: {raw!r}")
            return
        rotate = parse_rotation_from_filename(path)
        if rotate == 0 and self._rotate:
            rotate = self._rotate
        ok = self._mgr.show_image(path, rotate=rotate)
        if ok:
            log("display", f"wallpaper={path.name}" + (f" rotate={rotate}" if rotate else ""))
        else:
            log("display", f"wallpaper show failed: {path}")

    def _show_text(self, text: str) -> None:
        path = _render_text_image(text, self._rotate)
        if path is None:
            log("display", f"text render failed: {_preview(text)!r}")
            return
        if self._mgr is not None:
            ok = self._mgr.show_image(path, rotate=0)
            if ok:
                log("display", f"text={_preview(text)!r}")

    def _resolve_image_path(self, raw: str) -> Optional[Path]:
        raw = str(raw or "").strip()
        if not raw:
            return None

        module_dir = Path(__file__).resolve().parent
        candidates: list[Path] = []
        p = Path(raw).expanduser()
        if p.is_absolute():
            candidates.append(p)
        else:
            candidates.append(Path.cwd() / p)
            candidates.append(self._asset_dir / p)
            candidates.append(self._asset_dir / p.name)
            candidates.extend([
                module_dir / "display" / p,
                module_dir / "display" / p.name,
                module_dir.parent / "display" / p,
                module_dir.parent / "display" / p.name,
                Path.home() / "Revobots" / "display" / p,
                Path.home() / "Revobots" / "display" / p.name,
            ])

        seen: set[str] = set()
        for cand in candidates:
            key = str(cand)
            if key in seen:
                continue
            seen.add(key)
            if cand.is_file():
                return cand
        return None


def _resolve_display_name() -> str:
    if os.environ.get("DISPLAY"):
        return os.environ["DISPLAY"]
    for cand in (":0", ":10", ":1"):
        if os.path.exists(f"/tmp/.X11-unix/X{cand.lstrip(':')}"):
            return cand
    return ":0"


def _render_text_image(text: str, rotate: int) -> Optional[str]:
    """Render centered text to a PNG for MPV. Returns path or None."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        log("display", "Pillow not installed — cannot render display text")
        return None

    # Portrait-friendly canvas; MPV video-rotate handles orientation.
    width, height = (1080, 1920) if rotate in (90, 270) else (1920, 1080)
    bg = (18, 22, 32)
    panel = (24, 30, 44)
    fg = (240, 240, 245)

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)
    margin = width // 12
    draw.rounded_rectangle(
        (margin, margin, width - margin, height - margin),
        radius=28,
        fill=panel,
        outline=(180, 188, 200),
        width=3,
    )

    size = max(min(width, height) // 12, 42)
    font = None
    for path in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    ):
        if os.path.isfile(path):
            font = ImageFont.truetype(path, size)
            break
    if font is None:
        font = ImageFont.load_default()

    max_w = width - margin * 4
    lines = _wrap_text(text, font, max_w)
    line_h = size + 8
    total_h = line_h * len(lines)
    y = (height - total_h) // 2
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        tw = bbox[2] - bbox[0]
        draw.text(((width - tw) // 2, y), line, fill=fg, font=font)
        y += line_h

    try:
        img.save(_TEXT_IMAGE_PATH, "PNG")
        return _TEXT_IMAGE_PATH
    except OSError as exc:
        log("display", f"text image save failed: {exc}")
        return None


def _wrap_text(text: str, font, max_width: int) -> list[str]:
    lines: list[str] = []
    for paragraph in str(text or "").splitlines() or [""]:
        words = paragraph.split()
        if not words:
            lines.append("")
            continue
        current = words[0]
        for word in words[1:]:
            trial = f"{current} {word}"
            if font.getlength(trial) <= max_width:
                current = trial
            else:
                lines.append(current)
                current = word
        lines.append(current)
    return lines or [""]


def _preview(text: str, limit: int = 60) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."