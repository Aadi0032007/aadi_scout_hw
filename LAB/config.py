# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 20:04:03 2026

@author: Aadi
"""
from __future__ import annotations


# -*- coding: utf-8 -*-
"""LabConfig — REDESIGN.

Removed (from previous version):
    - udp_events_port  (was 57000 UDP; now TCP)
    - udp_tts_port     (was 57001; now folded into talk events on TCP)
    - daily_*          (Daily WebRTC replaced by MediaMTX pull from local GstRtspServer)
    - stream_*         (no compositor)
    - pip_*, overlay_* (no compositor)
    - mic_rtsp_*       (audio mic now handled by gst_rtsp audio branch)
    - initial_main_source, camera_name_aliases (no source switching)

Added:
    - tcp_events_port         57000, unified event channel (length-prefixed JSON, acked)
    - gst_rtsp_bind/_port     bind for the in-process GstRtspServer
    - gst_hw_encode           true → nvv4l2h264enc, false → x264enc (for USB stream only)
    - usb_stream_mount        mount name for the USB camera RTSP path
    - usb_stream_bitrate_*    bitrates for the two encoder branches
    - azure_telemetry_*       where telemetry UDP is pushed
    - telemetry_hz            telemetry publish rate

Camera list gains a `stream_only: bool` flag. Streaming-only cameras get an
RTSP passthrough factory in cameras.py and are NEVER opened locally.
USB (name="ai") stays with publish_frames=True and is the sole camera
teleop reads directly for record + AI + push-to-mount.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class CameraConfig:
    name:               str
    source:             str
    width:              int  = 640
    height:             int  = 480
    fps:                int  = 15
    rtsp_transport:     str  = "tcp"
    audio:              bool = True
    pixel_format:       str  = "MJPG"
    hw_decode:          bool = False
    publish_frames:     bool = False
    stream_only:        bool = False   # NEW — RTSP passthrough only, never opened locally

    @property
    def is_rtsp(self) -> bool:
        return isinstance(self.source, str) and self.source.startswith("rtsp://")

    def __post_init__(self) -> None:
        if self.rtsp_transport not in ("tcp", "udp"):
            raise ValueError(
                f"{self.name}: rtsp_transport must be 'tcp' or 'udp', "
                f"got {self.rtsp_transport!r}"
            )


@dataclass
class LabConfig:

    # ── Wire protocol ────────────────────────────────────────────────────────
    udp_listen_ip:          str = "0.0.0.0"
    udp_motion_port:        int = 55999      # trimmed motion payload, 50 Hz, unacked
    tcp_events_port:        int = 57000      # unified event channel, length-prefixed, acked

    # ── Source priority arbitration ──────────────────────────────────────────
    local_dongle_priority:  int  = 100
    remote_gamepad_priority: int = 200
    source_activity_timeout_sec: float = 1.0

    # ── Motion ───────────────────────────────────────────────────────────────
    docker_motion_host:     str   = "127.0.0.1"
    docker_motion_port:     int   = 56000
    motion_publish_hz:      int   = 50
    motion_watchdog_sec:    float = 0.30
    ang_z_scale:            float = 0.20
    brake_threshold:        float = 0.20

    # ── PTZ ──────────────────────────────────────────────────────────────────
    ptz_ip:                 str   = "192.168.10.50"
    ptz_port:               int   = 8000
    ptz_user:               str   = "revolabs"
    ptz_pan_speed:          float = 0.65
    ptz_tilt_speed:         float = 0.55
    ptz_loop_hz:            float = 25.0
    ptz_deadband_sec:       float = 0.05
    ptz_stop_after_sec:     float = 0.15

    # ── Lights / signals ─────────────────────────────────────────────────────
    blink_period_sec:       float = 0.40
    signal_timeout_sec:     float = 5.0
    talk_default_duration:  float = 7.0
    all_lights_cooldown_sec: float = 5.0
    all_lights_blink_sec:   float = 5.0

    # ── Audio ────────────────────────────────────────────────────────────────
    piper_model:            str   = str(Path.home() / "Revobots" / "piper" / "voices" / "en_GB-northern_english_male-medium.onnx")
    piper_speaker_id:       Optional[int] = None
    music_dir:              str   = str(Path.home() / "Revobots" / "audio")
    music_tracks: dict      = field(default_factory=lambda: {
        1: "REVOBOTS_Anthem_v1.wav",
        2: "REVO_Track_old1.wav",
        3: "REVO_Track_old2.wav",
    })
    startup_volume_pct:     int   = 100
    preferred_sink_patterns: list = field(default_factory=lambda: [
        "ugreen", "u_green", "usb_audio", "usb-audio", "emeet", "alsa_output.usb-",
    ])
    preferred_source_patterns: list = field(default_factory=lambda: [
        "ugreen", "u_green", "usb_audio", "usb-audio", "emeet", "alsa_input.usb-",
    ])

    # ── Cameras ──────────────────────────────────────────────────────────────
    # stream_only=True → RTSP passthrough only, never opened by cameras.py
    # publish_frames=True → USB, opened locally, frame bus + record + push to /usb-ai
    cameras: list = field(default_factory=lambda: [
        # RTSP passthrough cameras. Names/IPs match util_rtsp_server.py so
        # ffplay rtsp://<robot>:8556/<name> pulls the expected stream.
        # `audio=True` adds the AAC→Opus branch; set audio=False for video-only.
        CameraConfig(
            name="orbital",
            source="rtsp://admin:revolabs123%40@192.168.10.50:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=True,
            stream_only=True,
        ),
        CameraConfig(
            name="drive",
            source="rtsp://admin:revolabs123%40@192.168.10.51:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=False,
            stream_only=True,
        ),
        CameraConfig(
            name="rear",
            source="rtsp://admin:revolabs123%40@192.168.10.52:554/cam/realmonitor?channel=1&subtype=1",
            width=640, height=480, fps=15, rtsp_transport="udp",
            audio=False,
            stream_only=True,
        ),
        # USB AI camera. `audio` is ignored for USB (no audio branch in the
        # appsrc pipeline); mic capture is handled elsewhere.
        CameraConfig(
            name="ai",
            source="/dev/video2",
            width=640, height=480, fps=30,
            pixel_format="YUYV",
            publish_frames=True,
            hw_decode=True,
        ),
    ])

    # ── GstRtspServer (in-process, port 8556) ────────────────────────────────
    gst_rtsp_bind:              str  = "0.0.0.0"
    gst_rtsp_port:              int  = 8556
    gst_hw_encode:              bool = False           # true → nvv4l2h264enc, false → x264enc
    usb_stream_mount:           str  = "usb-ai"        # → rtsp://<robot>:8556/usb-ai
    usb_stream_bitrate_kbps:    int  = 1500            # x264enc bitrate (kbps)
    usb_stream_bitrate_bps:     int  = 1500000         # nvv4l2h264enc bitrate (bps)
    # Watchdog: found live that abandoned client TCP connections pile up in
    # CLOSE_WAIT. Same threshold + os._exit(1) pattern as the standalone.
    rtsp_close_wait_interval_sec: int = 30
    rtsp_close_wait_max:          int = 50

    # ── Telemetry: plain UDP to a fixed endpoint (Azure VM via Tailscale) ────
    # Cheap, no framing, drops-are-fine. Fields with no available source are
    # omitted from the packet.
    udp_telemetry_host:     str   = "100.94.48.1"   # Azure VM Tailscale IP
    udp_telemetry_port:     int   = 57100
    udp_telemetry_hz:       int   = 5

    # ── Sensors ──────────────────────────────────────────────────────────────
    imu_port_hint:          str   = "/dev/ttyCH341USB3"
    imu_baud:               int   = 9600
    gps_udp_host:           str   = "127.0.0.1"
    gps_udp_port:           int   = 57002

    # ── TEMPerHUM ────────────────────────────────────────────────────────────
    temphum_enabled:        bool  = True
    temphum_vid:            str   = "3553"
    temphum_pid:            str   = "A001"
    temphum_poll_sec:       float = 2.0
    temphum_stale_after_sec: float = 10.0

    # ── Battery ──────────────────────────────────────────────────────────────
    battery_enabled:        bool  = True
    battery_container:      str   = "segway_ros1"
    battery_topic:          str   = "/bms_fb"
    battery_ros_setup:      str   = "/opt/ros/noetic/setup.bash"
    battery_ws_setup:       str   = "/root/catkin_ws/devel/setup.bash"
    battery_poll_sec:       float = 5.0
    battery_cmd_timeout_sec: float = 3.0
    battery_stale_after_sec: float = 30.0

    # ── Lidar ────────────────────────────────────────────────────────────────
    lidar_enabled:          bool  = True
    lidar_symlink:          str   = "/dev/rplidar_s2"
    lidar_usb_serial:       str   = "4afc166e056ff011aec34b9b1045c30f"
    lidar_port:             str   = ""
    lidar_baud:             int   = 1_000_000
    lidar_poll_hz:          float = 2.0
    lidar_scan_timeout_sec: float = 3.0
    lidar_range_min_m:      float = 0.05
    lidar_range_max_m:      float = 18.0
    lidar_min_quality:      int   = 0
    lidar_front_min_deg:    float = -45.0
    lidar_front_max_deg:    float = 45.0
    lidar_left_min_deg:     float = 45.0
    lidar_left_max_deg:     float = 135.0
    lidar_right_min_deg:    float = -135.0
    lidar_right_max_deg:    float = -45.0
    lidar_bubble_front_m:   float = 0.10
    lidar_bubble_left_m:    float = 0.10
    lidar_bubble_right_m:   float = 0.10
    lidar_stale_after_sec:  float = 2.0
    lidar_safety_brake:     bool  = False

    # ── Recording ────────────────────────────────────────────────────────────
    cache_dir:              str   = os.path.expanduser("~/.cache/scout/lab")
    record_camera_name:     str   = "ai"
    record_width:           int   = 640
    record_height:          int   = 480
    record_fps:             int   = 15
    record_video_bitrate:   str   = "1500k"
    record_encoder_preference: list = field(default_factory=lambda: [
        "gst_nvenc",
        "libx264",
    ])

    # ── Local dongle ─────────────────────────────────────────────────────────
    local_dongle_enabled:   bool  = True
    local_dongle_name_hints: list = field(default_factory=lambda: [
        "8bitdo", "ultimate", "tgz", "cx 2.4g",
    ])

    # ── Secrets ──────────────────────────────────────────────────────────────
    ptz_password:           str   = ""
    camera_password:        str   = ""     # used by <camera-password> substitution

    @classmethod
    def load_secrets(cls, env_file: Optional[str] = None) -> "LabConfig":
        cfg = cls()
        if env_file is None:
            env_file = str(Path(__file__).parent / ".env")
        secrets = _read_env_file(env_file)
        cfg.ptz_password    = secrets.get("PTZ_PASSWORD", "")
        cfg.camera_password = secrets.get("CAMERA_PASSWORD", "")
        if secrets:
            print(f"[config] secrets loaded from {env_file}")
        else:
            print(f"[config] no .env at {env_file} — secrets empty")
        return cfg


def _read_env_file(path: str) -> dict:
    out: dict = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:]
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                out[key.strip()] = val.strip().strip("\"'")
    except FileNotFoundError:
        pass
    except Exception as exc:
        print(f"[config] error reading {path}: {exc}")
    return out