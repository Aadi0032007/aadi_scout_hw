# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


"""
Audio: PulseAudio volume, USB sink auto-selection, music playback, Piper TTS.

All operations are thread-safe and non-blocking. The command loop is never
delayed by audio work — synthesis and playback happen on background workers.

At startup:
    - Scans PulseAudio sinks/sources, picks UGREEN or EMEET USB device,
      unmutes it, sets it as the default. Same behavior as the original.

API:
    set_volume(pct)         — PulseAudio default-sink volume (0..100)
    speak(text)             — queue text for TTS
    play_music(track_num)   — play a WAV from the configured tracks dict
    stop_music()            — halt any currently playing music
"""

import os
import subprocess
import tempfile
import threading
import wave
from io import BytesIO
from pathlib import Path
from queue import Empty, Full, Queue
from typing import Optional

from .common import log


class AudioController:
    def __init__(
        self,
        piper_model:           str,
        music_dir:             str,
        music_tracks:          dict,
        startup_volume_pct:    int,
        preferred_sink_patterns:   list,
        preferred_source_patterns: list,
        piper_speaker_id:      Optional[int] = None,
    ) -> None:
        self._piper_model     = piper_model
        self._music_dir       = Path(music_dir)
        self._music_tracks    = music_tracks
        self._startup_volume  = startup_volume_pct
        self._sink_patterns   = preferred_sink_patterns
        self._source_patterns = preferred_source_patterns
        self._piper_speaker   = piper_speaker_id

        # TTS
        self._voice: Optional[object] = None   # PiperVoice instance, loaded once
        self._tts_queue: Queue[str] = Queue(maxsize=4)

        # Music
        self._music_proc: Optional[subprocess.Popen] = None
        self._music_lock = threading.Lock()

        # Lifecycle
        self._stop = threading.Event()
        self._tts_thread = threading.Thread(target=self._tts_worker, daemon=True, name="audio-tts")

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        self._init_pulseaudio_defaults()
        self.set_volume(self._startup_volume)
        self._load_piper()
        self._tts_thread.start()
        log("audio", "ready")

    def stop(self) -> None:
        self._stop.set()
        self.stop_music()

    # ── public API ────────────────────────────────────────────────────────────

    def set_volume(self, pct: int) -> None:
        """Set PulseAudio default-sink volume. Fire-and-forget."""
        pct = max(0, min(150, int(pct)))   # pactl supports up to 150% (software boost)
        threading.Thread(
            target=self._set_volume_impl,
            args=(pct,),
            daemon=True,
            name="audio-vol",
        ).start()

    def speak(self, text: str) -> None:
        """Queue text for TTS. Dropped silently if queue is full or model not loaded."""
        if not text or self._voice is None:
            return
        text = text.strip()
        if not text:
            return
        try:
            self._tts_queue.put_nowait(text)
        except Full:
            log("audio", "TTS queue full — dropping")

    def play_music(self, track_num: int) -> None:
        """Play a music track. Replaces any currently playing track."""
        filename = self._music_tracks.get(int(track_num))
        if not filename:
            log("audio", f"unknown music track: {track_num}")
            return

        path = self._music_dir / filename
        if not path.is_file():
            log("audio", f"missing music file: {path}")
            return

        with self._music_lock:
            self._kill_music_locked()
            cmd = self._build_player_cmd(str(path))
            try:
                self._music_proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                log("audio", f"playing track {track_num}: {path.name}")
            except Exception as exc:
                log("audio", f"music start failed: {exc}")
                self._music_proc = None

    def stop_music(self) -> None:
        with self._music_lock:
            self._kill_music_locked()

    # ── PulseAudio sink/source setup ──────────────────────────────────────────

    def _init_pulseaudio_defaults(self) -> None:
        sink = self._select_pactl_device("sinks", self._sink_patterns)
        if sink:
            self._run_pactl(["set-default-sink", sink])
            self._run_pactl(["set-sink-mute", sink, "0"])
            log("audio", f"default sink → {sink}")
        else:
            log("audio", "no preferred sink matched; using system default")

        source = self._select_pactl_device("sources", self._source_patterns)
        if source:
            self._run_pactl(["set-default-source", source])
            self._run_pactl(["set-source-mute",    source, "0"])
            self._run_pactl(["set-source-volume",  source, "100%"])
            log("audio", f"default source → {source}")

    def _select_pactl_device(self, kind: str, patterns: list) -> Optional[str]:
        """Return the first sink/source name whose lowercase contains any pattern."""
        names = self._list_pactl_devices(kind)
        if not names:
            return None
        lowered = [(n, n.lower()) for n in names]
        for pat in patterns:
            pat_l = pat.lower()
            for name, low in lowered:
                if pat_l in low:
                    return name
        return None

    def _list_pactl_devices(self, kind: str) -> list:
        rc, out = self._run_pactl(["list", "short", kind], capture=True)
        if rc != 0:
            return []
        names = []
        for line in out.splitlines():
            parts = line.split("\t")
            if len(parts) >= 2 and parts[1].strip():
                names.append(parts[1].strip())
        return names

    def _set_volume_impl(self, pct: int) -> None:
        self._run_pactl(["set-sink-mute",   "@DEFAULT_SINK@", "0"])
        rc, _ = self._run_pactl(["set-sink-volume", "@DEFAULT_SINK@", f"{pct}%"])
        if rc == 0:
            log("audio", f"volume → {pct}%")

    @staticmethod
    def _run_pactl(args: list, capture: bool = False) -> tuple[int, str]:
        try:
            res = subprocess.run(
                ["pactl"] + args,
                stdout=subprocess.PIPE if capture else subprocess.DEVNULL,
                stderr=subprocess.PIPE if capture else subprocess.DEVNULL,
                text=True,
                timeout=5,
            )
            return res.returncode, (res.stdout or "").strip()
        except Exception:
            return 1, ""

    # ── Music subprocess ──────────────────────────────────────────────────────

    @staticmethod
    def _build_player_cmd(path: str) -> list:
        """Pick the best available player. Same preference order as original."""
        for cmd in (
            ["paplay", path],
            ["pw-play", path],
            ["ffplay", "-nodisp", "-autoexit", "-loglevel", "error", path],
            ["aplay", "-q", path],
        ):
            if subprocess.run(["which", cmd[0]], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0:
                return cmd
        return ["aplay", "-q", path]

    def _kill_music_locked(self) -> None:
        proc = self._music_proc
        if proc is None:
            return
        self._music_proc = None
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass

    # ── Piper TTS ─────────────────────────────────────────────────────────────

    def _load_piper(self) -> None:
        """Load the Piper voice into memory once. ~250 MB RAM, ~2 s on first load."""
        if not self._piper_model:
            log("audio", "no Piper model configured — TTS disabled")
            return
        if not Path(self._piper_model).is_file():
            log("audio", f"Piper model not found: {self._piper_model}")
            return
        try:
            from piper.voice import PiperVoice
            self._voice = PiperVoice.load(self._piper_model)
            log("audio", f"Piper loaded: {Path(self._piper_model).name}")
        except ImportError:
            log("audio", "piper-tts not installed — pip install piper-tts")
        except Exception as exc:
            log("audio", f"Piper load failed: {exc}")

    def _tts_worker(self) -> None:
        while not self._stop.is_set():
            try:
                text = self._tts_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._synthesize_and_play(text)
            except Exception as exc:
                log("audio", f"TTS error: {exc}")
            finally:
                self._tts_queue.task_done()

    def _synthesize_and_play(self, text: str) -> None:
        if self._voice is None:
            return

        # Synthesize into an in-memory WAV buffer
        buf = BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)   # 16-bit PCM
            wf.setframerate(int(self._voice.config.sample_rate))
            kwargs = {}
            if self._piper_speaker is not None:
                kwargs["speaker_id"] = self._piper_speaker
            self._voice.synthesize_wav(text, wf, **kwargs)

        wav_bytes = buf.getvalue()
        if len(wav_bytes) <= 44:   # WAV header is 44 bytes — anything ≤ this is empty
            log("audio", "Piper produced empty WAV")
            return

        # Write to a temp file (players need a path)
        wav_path: Optional[str] = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".wav", prefix="lab_tts_", delete=False) as f:
                f.write(wav_bytes)
                wav_path = f.name
            self._play_wav(wav_path)
        finally:
            if wav_path:
                try:
                    os.unlink(wav_path)
                except OSError:
                    pass

    @staticmethod
    def _play_wav(path: str) -> None:
        for player in ("paplay", "pw-play", "aplay"):
            try:
                res = subprocess.run(
                    [player, "-q", path] if player == "aplay" else [player, path],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=30,
                )
                if res.returncode == 0:
                    return
            except FileNotFoundError:
                continue
            except Exception:
                continue
        log("audio", "no working player found")