# REVO Scout LAB

A single-process Python controller that turns a Jetson-based AGV into a
remotely operated telepresence robot. Replaces a sprawl of separate
`systemd`/ROS services with one orchestrator that ingests gamepad input
(local USB *or* remote UDP), drives `/cmd_vel`, controls a 4-channel light
relay and ONVIF PTZ, plays TTS and music, streams a composed
multi-camera view + microphone audio to a Daily.co room, and records a
synchronised MP4 + JSONL telemetry dataset every time the robot is
unlocked.

Everything below is current as of the last code revision. If something on
the physical robot doesn't match what's described here, the README is
wrong — please update it.

---

## Table of contents

1. [System architecture](#system-architecture)
2. [Repository layout](#repository-layout)
3. [Hardware inventory](#hardware-inventory)
4. [Prerequisites](#prerequisites)
5. [Configuration: `config.py` vs `.env`](#configuration-configpy-vs-env)
6. [Filesystem paths](#filesystem-paths)
7. [Pipelines](#pipelines)
   - [Motion](#motion-pipeline)
   - [Cameras](#camera-pipeline)
   - [Streaming](#streaming-pipeline)
   - [Recording](#recording-pipeline)
   - [Lights, signals, talk](#lights-signals-talk)
   - [PTZ](#ptz-pipeline)
   - [Audio (TTS / music / volume)](#audio-pipeline)
   - [Sensors (IMU + GPS + RTK)](#sensors-pipeline)
8. [Source arbitration & priorities](#source-arbitration--priorities)
9. [Gamepad controls](#gamepad-controls)
10. [Running the system](#running-the-system)
11. [Replicating to a new robot](#replicating-to-a-new-robot)
12. [Troubleshooting](#troubleshooting)

---

## System architecture

```
                         ┌──────────────────────────────────────────┐
                         │            Jetson (this code)            │
                         │                                          │
  ╔═══ Local USB ═════╗  │  ┌───────────────┐                       │
  ║  8BitDo Ultimate  ║──┼─▶│ local_gamepad │──┐                    │
  ╚═══════════════════╝  │  └───────────────┘  │                    │
                         │                     ▼                    │
                         │              ┌──────────────┐            │
                         │              │SourceArbiter │            │
                         │      ┌──────▶│ local prio   │            │
                         │      │       │      <       │            │
                         │      │  ┌───▶│ remote prio  │            │
                         │      │  │    └──────┬───────┘            │
                         │  UDP │  │           │                    │
  ╔═══ Tailscale ═════╗  │ 55999│  │           ▼                    │
  ║  Operator PC      ║──┼──────┘  │     ┌─────────┐                │
  ║  (pygame teleop)  ║  │  UDP 57000    │ motion  │──── /cmd_vel ──┼──▶ ROS2
  ╚═══════════════════╝──┼─────────┘     │   ptz   │──── ONVIF ─────┼──▶ camera
                         │               │ lights  │──── USB HID ───┼──▶ relay
                         │               │  audio  │──── PA / Piper │
                         │               │ stream  │──── WebRTC ────┼──▶ Daily
                         │               │ record  │──── ffmpeg ────┼──▶ ~/.cache
                         │               └────┬────┘                │
                         │                    ▲                     │
                         │             ┌──────┴──────┐              │
                         │  cameras ──▶│             │◀── sensors   │
                         │             │  in-process │   imu, gps   │
                         │             │  dispatch   │              │
                         │             └─────────────┘              │
                         └──────────────────────────────────────────┘
```

**One process, three UDP ports.** `teleop.py` is the foreground process.
It binds three UDP ports and starts every subsystem as a daemon thread.
The wire format is unchanged from the original operator script, so
deploying this on top of an existing fleet doesn't require any operator
side changes.

| Port    | Direction | Carries                                    |
|---------|-----------|--------------------------------------------|
| 55999   | inbound   | motion (`lin_x`, `ang_z`), `robot_lock`, `head`, `camera`, `button`, `speed` |
| 57000   | inbound   | events: `lights`, `signals`, `talk`, `audio`, `music` |
| 57001   | inbound   | TTS: `{"type":"stt","text":"..."}`          |
| 57002   | inbound   | NMEA fan-out from `gps_mux.py` (loopback only) |

**One ROS2 user.** Only `motion.py` calls `rclpy`. The orchestrator runs
`rclpy.init()` exactly once at startup and shares the global context
with the motion node. Every other subsystem talks directly to its
hardware. This keeps the dependency surface small and means the ROS2
graph only contains `lab_motion` publishing `geometry_msgs/Twist` to
`/cmd_vel`.

**Subsystem startup order** (in `teleop.main()`):

1. `LabConfig.load_secrets()` reads `LAB/.env`
2. `rclpy.init()` (if available)
3. `MultiCameraCapture` — one thread per RTSP/USB camera, 1-slot latest-frame buffer
4. `ImuReader` — UART, background thread, in-memory latest snapshot
5. `GpsReader` — UDP listener on 57002, fed by the `gps_mux` service
6. `MotionController` — creates the rclpy node, publishes at 50 Hz
7. `AudioController` — picks PulseAudio defaults, loads Piper model
8. `LightsController` — opens the HID relay, starts blink thread
9. `PtzController` — connects to ONVIF, starts pan/tilt loop
10. `DailyStream` — composes frames + audio, joins the room
11. `SessionRecorder` — constructed (does not start until robot unlocks)
12. Three `UdpListener`s bound
13. `LocalGamepad` started if `cfg.local_dongle_enabled`

Shutdown reverses this and runs unconditionally on `SIGINT`/`SIGTERM`.
Recording is finalised first, while cameras and sensors are still alive,
so the closing `session.json` row reflects real values.

---

## Repository layout

```
~/Revobots/aditya/aadi_scout/
├── LAB/
│   ├── __init__.py
│   ├── teleop.py            ← orchestrator, foreground process
│   ├── local_gamepad.py     ← local pygame controller (mirrors operator)
│   ├── config.py            ← every tunable + camera list + secrets loader
│   ├── common.py            ← log(), truthy(), first_float(), time helpers
│   ├── motion.py            ← UDP → /cmd_vel via rclpy
│   ├── cameras.py           ← MultiCameraCapture (RTSP + V4L2)
│   ├── stream.py            ← Daily.co virtual cam + virtual mic
│   ├── record.py            ← ffmpeg + JSONL session recorder
│   ├── ptz.py               ← ONVIF pan/tilt + dead-reckoned home
│   ├── lights.py            ← 4-channel USB HID relay
│   ├── audio.py             ← PulseAudio + Piper TTS + music
│   ├── sensors.py           ← IMU (WIT) + GPS (NMEA over UDP)
│   ├── .env                 ← secrets (never committed)
│   ├── .env.example         ← template
│   └── utils/
│       ├── gps_mux.py       ← single-owner GPS serial → PTY + UDP fan-out
│       └── (other helpers)
├── gps_rtk.sh               ← supervises gps_mux.py + Polaris RTK client
├── ros_start.sh             ← sources ROS2, launches agv_pro_bringup, runs teleop
└── README.md                ← this file
```

`gps_rtk.sh` and `ros_start.sh` live at the repo root. The two systemd
services (`aadi_gps_rtk.service`, `aadi_ros_start_teleop.service`) point
at them.

---

## Hardware inventory

| Device | Connection | Why the code needs it |
|---|---|---|
| AGV chassis (agv_pro) | ROS2 + Motor UART | `motion.py` publishes `/cmd_vel`; `agv_pro_bringup` translates to motor commands |
| WIT/JY901 IMU | UART (`/dev/ttyCH341USB3`) | `sensors.py` decodes 11-byte WIT frames |
| Unicore UM982 GPS | UART (`/dev/ttyCH341USB2`) | `gps_mux.py` reads NMEA + `#ADRNAVA` and fans out to PTY (for Polaris RTK) and UDP (for the rest of the robot) |
| Orbital PTZ camera | Ethernet, RTSP + ONVIF | Source of pilot video and the microphone audio stream |
| Front + rear AI cameras | USB (V4L2) | Side cameras for navigation; one of them is the recorded camera |
| Floor camera | USB (V4L2), symlinked at `/dev/floor_cam` | Close-up workspace cam |
| 4-channel USB HID relay (vid 0x16c0, pid 0x05df) | USB | `lights.py` toggles channels: 1=headlights, 2=strobe, 3=halo-L, 4=halo-R |
| USB audio (UGREEN or EMEET) | USB | `audio.py` selects this as the PulseAudio default sink/source |
| Local 8BitDo Ultimate / Ultimate 2 controller | USB | Optional. Local pilot when on-site |
| Operator PC | Same Tailscale network | Runs the unchanged `prod_revopilot_udp_...py` script |

---

## Prerequisites

### Operating system

- Ubuntu 22.04 LTS (tested on Jetson L4T 35.x; should also work on x86_64 for development)
- User account named `elephant` (or update the systemd units and `~` paths)

### ROS2

- ROS2 Humble installed at `/opt/ros/humble`
- Local workspace at `~/agv_pro_ros2/` containing the `agv_pro_bringup` package
  (`ros_start.sh` sources `install/local_setup.bash` from there)

### Python packages

```bash
# Core
pip install --break-system-packages \
  pyserial pyaudio numpy opencv-python pygame hid \
  onvif-zeep daily-python piper-tts

# rclpy comes with ROS2 — don't pip-install it
```

### System packages

```bash
sudo apt install -y \
  ffmpeg pulseaudio pulseaudio-utils alsa-utils \
  libhidapi-libusb0 libhidapi-hidraw0 \
  v4l-utils \
  python3-dev build-essential
```

### Groups

`elephant` must be in:

- `dialout` — to open `/dev/ttyCH341USB*` for the IMU and GPS
- `audio`   — to use PulseAudio
- `plugdev` — to talk to the USB HID relay

```bash
sudo usermod -aG dialout,audio,plugdev elephant
# log out + back in for the new groups to take effect
```

### udev rules (recommended)

Create stable device paths so the code doesn't break when ports shuffle:

```bash
# /etc/udev/rules.d/99-revo-scout.rules
KERNEL=="ttyCH341USB*", ATTRS{idVendor}=="1a86", MODE="0660", GROUP="dialout"
SUBSYSTEM=="video4linux", ATTRS{idVendor}=="<vendor>", ATTRS{idProduct}=="<product>", SYMLINK+="floor_cam"
SUBSYSTEM=="hidraw", ATTRS{idVendor}=="16c0", ATTRS{idProduct}=="05df", MODE="0660", GROUP="plugdev"
```

After editing: `sudo udevadm control --reload-rules && sudo udevadm trigger`.

### Linger (for PulseAudio at boot)

`audio.py` talks to the per-user PulseAudio daemon. For the teleop
service to find that daemon at boot — before any human logs in — enable
linger:

```bash
sudo loginctl enable-linger elephant
```

This makes systemd start the user-bus session at boot. Without it the
first `pactl` call after a reboot fails until you SSH in.

### Piper TTS voices

Download a voice model and place it where `config.py` points:

```bash
mkdir -p ~/Revobots/piper/voices
cd ~/Revobots/piper/voices
# example: northern English male
wget https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/northern_english_male/medium/en_GB-northern_english_male-medium.onnx
wget https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_GB/northern_english_male/medium/en_GB-northern_english_male-medium.onnx.json
```

### Music tracks

Place WAV files at `~/Revobots/Audio/`:

```
~/Revobots/Audio/
├── REVOBOTS_Anthem_v1.wav      ← track 1
├── REVO_Track_old1.wav          ← track 2
└── REVO_Track_old2.wav          ← track 3
```

The mapping `{1: "filename.wav", ...}` lives in `LabConfig.music_tracks`.

### Point One Polaris (RTK)

```bash
git clone https://github.com/PointOneNav/polaris ~/Revobots/polaris
cd ~/Revobots/polaris && mkdir build && cd build
cmake .. && make -j
# The supervisor expects: ~/Revobots/polaris/build/examples/serial_port_client
```

### Daily.co room

- Create a room at https://dashboard.daily.co (free tier works)
- Note the room URL (e.g. `https://your-team.daily.co/room-name`)
- Grab a Daily API key from the dashboard
- Both go in `LAB/.env`

---

## Configuration: `config.py` vs `.env`

There are exactly two places where the robot is configured.

### `LAB/config.py` — everything non-secret

`LabConfig` is one dataclass with every tunable. Camera URLs, ports,
audio file paths, FPS, blink periods, encoder preferences, the PTZ home
button number — everything that defines what this robot is and how it
behaves. **Edit this file to retune the robot.** It's not generated and
not in `.env` because it benefits from being in version control: every
robot's tuning history is visible in git.

### `LAB/.env` — secrets only

Four lines, never committed. Copy from `.env.example`:

```
DAILY_API_KEY=dk_xxxxxxxxxxxxxxxxxxxxx
PTZ_PASSWORD=YourPtzPassword
POINTONE_API_KEY=p1_xxxxxxxxxxxxxxxxxxxx
POLARIS_UNIQUE_ID=robot-elephant-01
```

`LabConfig.load_secrets()` reads `DAILY_API_KEY` and `PTZ_PASSWORD` and
splices them onto the config object. `POINTONE_API_KEY` and
`POLARIS_UNIQUE_ID` are read by `gps_rtk.sh` directly via
`set -a; source LAB/.env; set +a`.

---

## Filesystem paths

These are the paths the code reads from or writes to. Anything not
listed is internal/scratch.

| Path | Direction | What's there |
|------|-----------|--------------|
| `~/.cache/scout/lab/` | write | Dataset root. One folder per recording session: `session_YYYYMMDD_HHMMSS_<rand>/` containing `video.mp4`, `data.jsonl`, `session.json` |
| `~/Revobots/piper/voices/` | read | Piper voice `.onnx` + `.onnx.json` |
| `~/Revobots/Audio/` | read | Music tracks (`.wav`) listed in `LabConfig.music_tracks` |
| `~/Revobots/polaris/build/examples/serial_port_client` | read/exec | Point One RTK binary |
| `~/Revobots/aditya/aadi_scout/LAB/.env` | read | Secrets |
| `/tmp/scoutlab_gps_pty` | read/write (Polaris) | PTY symlink created by `gps_mux.py` so Polaris can talk RTCM corrections back to the GPS |
| `/tmp/scoutlab_gps_rtk.lock` | write | flock guard preventing two RTK supervisors from running |
| `/dev/ttyCH341USB2` | read/write | GPS UART (owned by `gps_mux.py`) |
| `/dev/ttyCH341USB3` | read | IMU UART |
| `/dev/floor_cam`, `/dev/video2`, `/dev/video8` | read | USB cameras |
| `/dev/shm/fastrtps*` | wiped at startup | FastDDS shared memory locks (cleared by `ros_start.sh`) |

### Session folder anatomy

```
~/.cache/scout/lab/session_20260603_143012_abc/
├── video.mp4          ← H.264, 640×480 @ 15 fps, ffmpeg-encoded
├── data.jsonl         ← one row per recorded frame
└── session.json       ← summary written on session close
```

`data.jsonl` rows look like:

```json
{"frame_index": 0, "t_unix": 1717420212.04, "t_mono": 18234.51,
 "lin_x": 0.0, "ang_z": 0.0, "locked": false, "braking": false,
 "imu": {"roll": -1.2, "pitch": 0.4, "yaw": 178.6, ...},
 "gps": {"gps_latitude": 51.5074, "gps_longitude": -0.1278,
         "gps_fix": "RTK_FIXED", "gps_satellites": 18, ...}}
```

`session.json`:

```json
{"session_dir": "...",   "start_unix": 1717420212.0,
 "start_iso":   "2026-06-03T14:30:12",
 "fps": 15,              "frame_count": 4521,
 "duration_sec": 301.4,  "encoder": "libx264",
 "width": 640,           "height": 480,
 "video": "video.mp4",   "telemetry": "data.jsonl",
 "camera": "ai_front"}
```

`frame_index` in `data.jsonl` aligns 1:1 with frames in `video.mp4`, so
you can decode frame *N* and pair it with telemetry row *N* without any
timestamp matching.

---

## Pipelines

### Motion pipeline

```
UDP 55999 ─▶ on_motion_packet ─▶ SourceArbiter ─▶ MotionController.command()
                                                          │
                                                          ▼
                                                  publish_loop @ 50 Hz
                                                          │
                                                          ▼
                                              geometry_msgs/Twist → /cmd_vel
```

`MotionController` runs its own publisher loop at 50 Hz and applies
three gates before publishing:

1. **Watchdog** — if no `command()` arrived within `motion_watchdog_sec`
   (300 ms), publish zero. Survives operator-side hangs and packet loss.
2. **Robot lock** — `robot_lock=true` forces zero output.
3. **Brake** — `brake > brake_threshold` forces zero output. (The
   operator script signals "brake" by pressing both cruise buttons at
   once.)

`ang_z` is multiplied by `ang_z_scale` (default 0.20) so turning feels
proportional to forward speed instead of pivoting the chassis. On
shutdown three zeros are published 20 ms apart for safety.

### Camera pipeline

```
RTSP / V4L2 source ─▶ CameraCapture thread ─▶ 1-slot latest-frame buffer
                                                       │
                                                       ▼
                                  read_latest()  →  stream + record consumers
```

Each `CameraCapture` is one daemon thread. It reads as fast as the
source produces frames, but only keeps the *latest* in memory — older
frames are silently overwritten. Consumers (stream compositor,
recorder) call `read_latest()` which is non-blocking and always returns
immediately. This means producers and consumers run at independent
rates and slow consumers never block fast producers.

RTSP sources get FFmpeg low-latency options applied transiently via the
`OPENCV_FFMPEG_CAPTURE_OPTIONS` environment variable. USB sources use
the V4L2 backend with `CAP_PROP_BUFFERSIZE=1` to avoid kernel-side
frame queuing.

Reconnect is automatic with exponential backoff (1 s → 10 s capped).
Cameras that fail their initial probe-open are simply absent from the
collection — the orchestrator continues with whatever opened.

### Streaming pipeline

```
cameras ──┬─▶ pick main ──┐
          ├─▶ pick PiP-L ─┤
          └─▶ pick PiP-R ─┤
                          ▼
                  compose 640×480 BGR frame
                          │
                  draw badges (speed/cam/timestamp)
                          │
                  BGR → RGB conversion
                          │
                  Daily virtual cam @ 15 fps
                          ▼
                   Daily.co room

orbital RTSP audio ─▶ ffmpeg s16le 16k mono ─▶ Daily virtual mic
```

The main view is the camera selected via the operator's gamepad
(`camera` field in the motion packet). Two PiP thumbnails sit in the
upper corners — by default left = orbital (pilot), right = ai_back
(rear). Camera name and current speed level are rendered as small
badges at the bottom.

Audio comes from the **orbital camera's RTSP stream**, not from a local
microphone. `ffmpeg` decodes the AAC track to 16 kHz mono s16le and
pipes it into `Daily.write_frames()`. This means the operator hears
what's around the robot's head, not what's around the Jetson.

When `robot_lock=True`, the stream freezes on the last video frame and
drains-but-drops audio. Locking is a hard visual indicator that the
robot is not responsive.

### Recording pipeline

```
robot_lock False→True edge   ───▶  recorder.start()
                                       │
                                       ▼
                            mkdir session_<timestamp>
                                       │
                            probe encoders, start ffmpeg
                                       │
                            open data.jsonl
                                       │
                                       ▼
                               tick_loop @ record_fps
                                       │
                            ┌──────────┴──────────┐
                            ▼                     ▼
                       frame to ffmpeg     JSONL row with
                                          motion + imu + gps

robot_lock True→False edge   ───▶  recorder.stop()
   OR SIGINT/SIGTERM                     │
                                         ▼
                          close ffmpeg stdin (finalises MP4)
                                         │
                                  close data.jsonl
                                         │
                                  write session.json
                                         │
                                  reset state, ready for next session
```

**Encoder probing**: `record.py` walks `cfg.record_encoder_preference`
in order and uses the first one that successfully starts. The default
list in `config.py` is `["libx264"]`. On Jetson with NVENC available
you can prepend `"h264_nvenc"` or `"h264_v4l2m2m"` to offload encoding
from the CPU.

**Frame-index alignment**: each frame Index *N* written to ffmpeg gets
exactly one row in `data.jsonl` with `"frame_index": N`. There is no
timestamp matching at analysis time — frame N in the MP4 is row N in
the JSONL, full stop.

**Auto-start on unlock**: recording is gated entirely by `robot_lock`.
There's no UI toggle, no keyboard shortcut. Unlock the robot, recording
starts. Lock it, recording stops and `session.json` is written. Ctrl-C
also stops cleanly because the recorder's `stop()` runs in the shutdown
finally-block before cameras die.

### Lights, signals, talk

The 4-channel USB HID relay sees three independent animations that
coexist without fighting each other:

- **Steady state** — headlights, strobe, halos-as-parking-lights are
  independent on/off flags set by `event:"lights"` packets.
- **Turn signal** — `event:"signals"` with `left:true` or `right:true`
  blinks one halo for `signal_timeout_sec` (20 s default) without
  touching headlights or strobe.
- **All-blink** — `event:"talk"` blinks all four channels together for
  a configurable duration. The "all lights on" combo (`headlights:true,
  parklights:true, strobe:true`) blinks for 5 s then latches everything
  on.

Precedence inside the blink thread is: all-blink > turn signal >
steady. One thread, one tick — there's no race between animations.

`robot_lock=True` forces every channel off and locks out further
commands. HID auto-reconnects on USB transients.

### PTZ pipeline

ONVIF `ContinuousMove` with dead-reckoned position. The `head` field
in motion packets drives pan/tilt:

| `head` value | Action |
|---|---|
| `"left"` | pan negative at `pan_speed` |
| `"right"` | pan positive at `pan_speed` |
| `"up"` | tilt positive at `tilt_speed` |
| `"down"` | tilt negative at `tilt_speed` |
| `"center"` | stop |

Position is integrated as `velocity * dt` each tick. That's accurate
enough for "look around then return roughly to where I started", not
for precision pointing.

**Home capture triggers**:

- First unlock — first time the robot transitions to unlocked
- Speed-level cycle (A→B while already unlocked)
- A+B button combo
- Explicit `ptz.capture_home()` call

**Home return**: button 8 (lights-on button on the gamepad) calls
`ptz.goto_home()` which drives back toward stored origin until inside
the deadband.

**Important**: PTZ has its own independent lock state. The drivetrain
`robot_lock` does **not** stop the PTZ, so the operator can still look
around while the robot is locked. This is intentional — you often want
to assess surroundings before unlocking.

### Audio pipeline

Three independent capabilities sharing one `AudioController`.

**PulseAudio setup at startup**:

1. List sinks/sources via `pactl list short`
2. Find first whose name contains any of `cfg.preferred_sink_patterns`
   (`ugreen`, `u_green`, `emeet`, `alsa_output.usb-`, …)
3. Set it as default, unmute it, set volume to `cfg.startup_volume_pct`
4. Same for the source (microphone)

**Piper TTS**:

- Voice model loaded once at startup (~250 MB RAM, ~2 s)
- `speak(text)` queues to a 4-slot bounded queue (drops on overflow)
- Background worker synthesises to in-memory WAV, writes to temp file,
  plays via the first available of `paplay` / `pw-play` / `aplay`

**Music**:

- `play_music(track_num)` looks up `cfg.music_tracks[track_num]`,
  replaces any currently playing track, plays via the same player
  preference chain
- `stop_music()` terminates the player subprocess

**Volume**:

- `set_volume(pct)` accepts 0–150 (PulseAudio supports up to 150% via
  software boost) and applies to `@DEFAULT_SINK@`

All audio work happens on background threads — the UDP dispatcher
never blocks waiting for synthesis or playback.

### Sensors pipeline

**IMU** (`ImuReader`):

- Reads `/dev/ttyCH341USB3` at 9600 baud
- WIT protocol: 11-byte frames `[0x55, frame_id, 8 bytes payload, checksum]`
- Decodes frame IDs 0x51 (accel), 0x52 (gyro), 0x53 (RPY), 0x54 (mag),
  0x59 (quaternion)
- Background thread, in-memory dict, `get()` returns latest snapshot

**GPS** (`GpsReader` + `gps_mux.py`):

```
/dev/ttyCH341USB2  (115200 baud)
       │
       ▼
   gps_mux.py  (single owner)
       ├──▶  /tmp/scoutlab_gps_pty  (Polaris reads/writes here)
       │            │
       │            ▼
       │    Polaris RTK client  ──▶  RTCM corrections back to GPS
       │
       └──▶  UDP 127.0.0.1:57002  (NMEA fan-out)
                    │
                    ▼
              GpsReader (in teleop process)
                    │
                    ▼
              recorder + stream + telemetry
```

`gps_mux.py` owns the serial port and gives two consumers a view of it.
This solves a real problem: only one process can hold the GPS UART, but
both Polaris (for RTK corrections) and teleop (for position) need it.
The mux is the single owner; everyone else gets a derivative.

Fix labels (`gps_fix`): `NO_FIX`, `GPS_FIX`, `DGPS_FIX`, `RTK_FIXED`,
`RTK_FLOAT`, `ESTIMATED`. `RTK_FIXED` is the gold standard — sub-cm
position with RTK corrections successfully applied.

---

## Source arbitration & priorities

Two command sources can be active simultaneously: the local USB
gamepad and the remote operator over UDP.

| Source | Priority value | Active when |
|---|---|---|
| Local (USB gamepad on Jetson) | **100** (lower = wins) | Sending motion packets within the last `source_activity_timeout_sec` (1.0 s default) |
| Remote (UDP from operator PC) | **200** | Sending motion packets within the timeout |

When both are active, **local wins** — the on-site operator overrides
the remote one. If the local pilot puts the controller down for >1 s,
the remote takes over automatically. No mode switch, no UI gesture.

**Important: only motion packets are arbitrated.** Light, signal, talk,
audio, music and TTS events fire from both sources unconditionally. So
if you press the local lights button while the remote operator presses
their lights button, both events arrive at `LightsController` — but
since lights commands are idempotent, no harm done.

---

## Gamepad controls

These mappings apply to **both** the local USB controller and the
remote operator PC. They're literally the same code — `local_gamepad.py`
imports the same mapping tables that the operator script uses.

### Sticks & axes

| Control | 8BitDo Ultimate (PC) | 8BitDo Ultimate 2 | Action |
|---|---|---|---|
| Steering wheel | axis 0 | axis 0 | `ang_z` (turning) |
| Indicator axis | axis 4 | axis 3 | Edge-trigger: ≤-30000 = left signal, ≥+30000 = right signal |
| Sound axis | axis 3 | axis 4 | Edge-trigger: see below |
| Head pan (L/R) | axis 6 | axis 6 | PTZ left/right |
| Head tilt (U/D) | axis 7 | axis 7 | PTZ up/down |
| Lift positive | axis 4 | axis 5 | Lift up |
| Lift negative | axis 5 | axis 2 | Lift down |

### Buttons

| Action | 8BitDo Ultimate (PC) | 8BitDo Ultimate 2 |
|---|---|---|
| A | button 0 | button 0 |
| B | button 1 | button 1 |
| X | button 3 | button 2 |
| Y | button 4 | button 3 |
| Cruise up | button 7 | button 5 |
| Cruise down | button 6 | button 4 |
| Lights ON | button 11 | button 7 |
| Lights OFF | button 10 | button 6 |

The mapping table includes a third profile,
`8bitdo_ultimate2_wireless_windows`, used when running the operator
script on Windows. The Jetson never hits this branch.

### Lock / unlock / speed sequences

Multi-press sequences are detected within a 2-second window.

| Sequence | Effect |
|---|---|
| **A → B** (while locked) | Unlock robot, set max speed = 1.0 m/s (slow). Captures PTZ home. |
| **A → B** (while unlocked) | Cycle speed level: slow (1.0) → medium (2.0) → fast (3.0) → slow. Re-captures PTZ home. |
| **B → A** | Lock robot. All motion goes to zero. Lights stay as they are. |

The "captures PTZ home" part means the orbital camera's current
pan/tilt position is stored — pressing the lights-ON button later
returns to that pose.

### Per-button actions

| Button | Edge action |
|---|---|
| **Lights ON** | Send `{event:"lights", headlights:true, parklights:true, strobe:true}` 10× over UDP; also acts as the "PTZ home" button (returns orbital camera to stored home) |
| **Lights OFF** | Send `{event:"lights", headlights:false, parklights:false, strobe:false}` 10× |
| **X** (when not in a lock sequence) | Cycle camera forward: floor → orbital → ai_front → ai_back → floor … |
| **Y** (when not in a lock sequence) | Cycle camera backward |
| **Cruise up** | Bump cruise speed up one notch (13 levels, -1.0 to +1.0) |
| **Cruise down** | Bump cruise speed down one notch |
| **Both cruise pressed** | Hard brake — clamp `lin_x` to 0 and reset cruise to 0 |

### Indicator axis (axis 3 or 4 depending on controller)

| Direction | Action |
|---|---|
| Push left (axis < -30000) | Send `{event:"signals", left:true, right:false}` — left turn signal blinks |
| Push right (axis > +30000) | Send `{event:"signals", left:false, right:true}` — right turn signal blinks |
| Center | No-op (signals self-cancel after `signal_timeout_sec`) |

### Sound axis (axis 4 or 3 depending on controller)

| Action | Effect |
|---|---|
| Single push down (one tap) | Wait 1 s, then send `audio volume 100%` + `talk 7 s` + speak "Hellow how are you today?" |
| Double tap down (within 1 s) | Send `music play track 1` + `talk 60 s` (60 s of all-blink lights while music plays) |
| Single push up | Immediately send `audio volume 100%` + `talk 7 s` + speak "please let me go!" |

These are the operator-side defaults. Change the messages in
`prod_revopilot_udp_...py` and in `LAB/local_gamepad.py` (search for
`AXIS4_NEG_MESSAGE` / `AXIS4_POS_MESSAGE`).

### Payload `button` field

Every motion packet carries a `button` integer indicating the currently
pressed button (highest-priority one if multiple):

| Value | Button |
|---|---|
| 0 | none |
| 1 | A |
| 2 | B |
| 3 | X |
| 4 | Y |
| 5 | cruise down |
| 6 | cruise up |
| 7 | lights off |
| 8 | lights on |

The robot uses **button 8** as the "PTZ go-home" trigger. This is set
by `cfg.ptz_home_button` in `config.py`.

---

## Running the system

### As services (production)

Two systemd units, in `/etc/systemd/system/`:

- `aadi_gps_rtk.service` — runs `gps_mux.py` + Polaris RTK client
- `aadi_ros_start_teleop.service` — runs `ros_start.sh` (sources ROS2,
  launches `agv_pro_bringup`, runs `teleop.py`)

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now aadi_gps_rtk.service
sudo systemctl enable --now aadi_ros_start_teleop.service

# Status + logs
systemctl status aadi_gps_rtk.service
systemctl status aadi_ros_start_teleop.service
journalctl -u aadi_gps_rtk.service -f
journalctl -u aadi_ros_start_teleop.service -f
```

Both services have `WantedBy=multi-user.target` so they start at boot.
`aadi_ros_start_teleop.service` has `After=aadi_gps_rtk.service` so
GPS comes up first.

### Interactively (development)

Stop the services first if they're running:

```bash
sudo systemctl stop aadi_ros_start_teleop.service
sudo systemctl stop aadi_gps_rtk.service
```

Then:

```bash
cd ~/Revobots/aditya/aadi_scout
set -a; source LAB/.env; set +a
./gps_rtk.sh &              # in one terminal
bash ros_start.sh           # in another
```

To bypass the local gamepad (e.g. when no controller is plugged in but
pygame keeps printing errors):

```bash
python3 -m LAB.teleop --no-local-dongle
```

### Verifying things work

- **GPS**: `journalctl -u aadi_gps_rtk -f` should show NMEA fanned out.
  After ~30 s with sky view: `gps_fix` should transition to `RTK_FIXED`.
- **ROS**: `ros2 topic echo /cmd_vel` — should publish at 50 Hz when the
  robot is unlocked and receiving commands.
- **Cameras**: `journalctl -u aadi_ros_start_teleop -f` will show
  `[cameras] orbital: started 640x480@15fps (RTSP)` etc. as each opens.
- **Daily stream**: open the Daily room URL — you should see the
  composed view inside ~5 s of teleop starting.
- **Recording**: unlock the robot. A new folder appears under
  `~/.cache/scout/lab/`. Lock the robot — `session.json` is created.

---

## Replicating to a new robot

This is the canonical checklist for cloning the codebase to a new
machine. Run through it top-to-bottom; if you skip a step it usually
shows up as an unhelpful error 20 minutes later.

### 1. OS + user

```bash
# Create the elephant user (or whichever name you'll standardise on)
sudo adduser elephant
sudo usermod -aG dialout,audio,plugdev,sudo elephant
sudo loginctl enable-linger elephant
```

### 2. System packages

```bash
sudo apt update
sudo apt install -y \
  python3-pip ffmpeg \
  pulseaudio pulseaudio-utils alsa-utils \
  libhidapi-libusb0 libhidapi-hidraw0 \
  v4l-utils \
  build-essential python3-dev cmake git
```

### 3. ROS2

Install ROS2 Humble per the official instructions. Then clone your
`agv_pro_ros2` workspace to `~/agv_pro_ros2/` and build it:

```bash
cd ~/agv_pro_ros2
colcon build --symlink-install
```

### 4. Clone this repo

```bash
mkdir -p ~/Revobots/aditya
cd ~/Revobots/aditya
git clone <your-repo-url> aadi_scout
cd aadi_scout
chmod +x gps_rtk.sh ros_start.sh
```

### 5. Python deps

```bash
pip install --break-system-packages \
  pyserial pyaudio numpy opencv-python pygame hid \
  onvif-zeep daily-python piper-tts
```

### 6. Piper voice + music

```bash
mkdir -p ~/Revobots/piper/voices ~/Revobots/Audio
# Download voice .onnx + .onnx.json into ~/Revobots/piper/voices/
# Copy your .wav tracks into ~/Revobots/Audio/
```

### 7. Polaris RTK client

```bash
git clone https://github.com/PointOneNav/polaris ~/Revobots/polaris
cd ~/Revobots/polaris && mkdir build && cd build && cmake .. && make -j
```

### 8. udev rules

Write `/etc/udev/rules.d/99-revo-scout.rules` with stable names for the
GPS, IMU, USB cameras, and HID relay. Reload udev:

```bash
sudo udevadm control --reload-rules && sudo udevadm trigger
```

Then verify:

```bash
ls -l /dev/ttyCH341USB2 /dev/ttyCH341USB3 /dev/floor_cam
ls -l /dev/hidraw*  # the relay should be group-readable for plugdev
```

### 9. Edit `LAB/config.py` for THIS robot

This is the part most often skipped. Walk through `LabConfig` field
by field and update:

- `ptz_ip` — the orbital camera's IP on this robot's LAN
- `ptz_user`, `ptz_password` *(password goes in `.env`)*
- `cameras = [...]` — every `CameraConfig` source. RTSP URLs change
  per robot (different orbital camera IP), USB device paths can vary
- `daily_room_url`, `daily_room_name` — each robot needs its own Daily
  room or operators will see each other's feeds
- `mic_rtsp_url` — same as the orbital camera's RTSP URL
- `imu_port_hint`, `gps_udp_port` — only change if your udev rules use
  different symlinks
- `record_encoder_preference` — on Jetson with NVENC, prepend
  `"h264_nvenc"` for hardware encoding
- `music_tracks` — if you renamed/added music files
- `piper_model` — if you use a different voice
- `preferred_sink_patterns`, `preferred_source_patterns` — if your USB
  audio device isn't UGREEN or EMEET, add a substring of its
  PulseAudio name here

### 10. Create `LAB/.env`

```bash
cp LAB/.env.example LAB/.env
nano LAB/.env
```

Fill in:

```
DAILY_API_KEY=...        # from Daily.co dashboard
PTZ_PASSWORD=...         # this robot's orbital camera password
POINTONE_API_KEY=...     # from Point One dashboard
POLARIS_UNIQUE_ID=...    # a STABLE per-robot string, e.g. "scout-elephant-01"
```

**The `POLARIS_UNIQUE_ID` must be different on every robot** — Point
One uses it to deduplicate clients on their virtual reference station
network. Two robots with the same ID will fight each other for RTK
corrections.

### 11. Install the systemd units

```bash
# Write the two unit files
sudo nano /etc/systemd/system/aadi_gps_rtk.service
sudo nano /etc/systemd/system/aadi_ros_start_teleop.service
```

Paste the unit content from this repo. Then:

```bash
# Check the UID matches what's in the teleop unit
id -u elephant      # if not 1000, edit Environment=XDG_RUNTIME_DIR=/run/user/<UID>

sudo systemctl daemon-reload
sudo systemctl enable --now aadi_gps_rtk.service
sudo systemctl enable --now aadi_ros_start_teleop.service
```

### 12. Operator-side config

On the **operator PC** (not the robot), edit
`prod_revopilot_udp_highlowfreq_gamepad_cmds_win_lin.py`:

```python
ROBOT_IPS = {
    "ELEPHANT": "100.80.7.54",       # ← Tailscale IP of THIS robot
    ...
}
DEFAULT_ROBOT = "ELEPHANT"
```

Add an entry for the new robot, set it as default if it's becoming the
primary, and run the script.

### 13. Smoke test

```bash
# 1. Does GPS come up?
sudo systemctl status aadi_gps_rtk
journalctl -u aadi_gps_rtk -n 50 | grep -E '(opened|PTY|NMEA)'

# 2. Does teleop come up?
sudo systemctl status aadi_ros_start_teleop
journalctl -u aadi_ros_start_teleop -n 100 | grep -E '(ready|started|connected)'

# 3. Connect operator, unlock robot
#    → /cmd_vel should publish at 50 Hz
#    → a new session folder should appear in ~/.cache/scout/lab/
#    → Daily room should show the live composed view

# 4. Lock robot, Ctrl-C the service (just to verify)
sudo systemctl stop aadi_ros_start_teleop
ls ~/.cache/scout/lab/session_*/session.json    # must exist
```

### 14. Optional but recommended

- Set up a **log rotation** policy for the `~/.cache/scout/lab/`
  dataset folder. A 30-minute session at 15 fps is ~50 MB of video
  plus a few MB of JSONL — easily a gig per day if you record a lot.
- Pin Python package versions in a `requirements.txt` so reinstalls
  don't drift.
- Consider a **read-only `git` checkout** on the robot, with a separate
  config repo for the per-robot `.env` and `config.py`. Avoids
  accidentally committing one robot's IPs to another robot's branch.
- Set the **robot hostname** to something memorable
  (`sudo hostnamectl set-hostname scout-elephant-01`) so journalctl
  logs are unambiguous when you SSH between machines.

---

## Troubleshooting

### `session.json` sometimes missing after Ctrl-C

Fixed as of the last revision. The bug was that `record.py`'s `stop()`
early-returned when `_active` was already `False` (typically because
the ffmpeg pipe died mid-session). The current code uses
`_session_dir is None` as the gate, so any session that opened a folder
will get its `session.json` written on shutdown, regardless of internal
state. If you see this happen on the current code, check the journal
for `[record] session.json write error` lines.

### Lights don't respond / flash randomly

- Confirm the HID relay is plugged in: `lsusb | grep 16c0`
- Check `elephant` is in the `plugdev` group: `groups elephant`
- Check `/dev/hidraw*` is readable by your user:
  `ls -l /dev/hidraw*`
- If the relay is shared with another process (e.g. a stale Python),
  HID writes will silently fail. `lsof /dev/hidraw0` (substitute the
  right number) shows owners.

### Cameras don't open

- USB: `v4l2-ctl --list-devices` and verify the device path in
  `config.py` matches.
- RTSP: try `ffplay rtsp://...` from the Jetson to confirm the URL
  works outside OpenCV.
- Check `cfg.cameras[i].rtsp_transport` — switch between `"tcp"` and
  `"udp"` if you're behind a flaky link.

### GPS shows `NO_FIX` indefinitely

- Confirm the antenna has clear sky view (RTK needs >180° of sky).
- `journalctl -u aadi_gps_rtk -f | grep ADRNAVA` — should show position
  updates within ~30 s of fix acquisition.
- Check `POINTONE_API_KEY` and `POLARIS_UNIQUE_ID` are set in `.env`
  and that `set -a; source LAB/.env; set +a` exports them. The
  supervisor errors loudly if they're missing.

### Audio is silent

- Linger enabled? `loginctl show-user elephant | grep Linger`
- Default sink correct? `pactl info | grep "Default Sink"`
- Volume non-zero? `pactl get-sink-volume @DEFAULT_SINK@`
- If you just switched USB ports, `audio.py` only matches sinks at
  startup — restart the teleop service after re-plugging.

### Daily stream not connecting

- `DAILY_API_KEY` in `.env`?
- `cfg.daily_room_url` matches what you created in the Daily
  dashboard?
- Outbound UDP not blocked? Daily uses WebRTC over UDP. On a
  restrictive network you may need to allow ports 16384-32767 outbound.

### Operator UDP packets arrive but robot doesn't move

- Confirm packets reach the listener:
  `sudo tcpdump -i any udp port 55999`
- Confirm arbiter is picking the right source — packets with no
  `_local: true` get tagged as remote. `journalctl -f` will show source
  switches.
- Check `motion._locked`. The robot starts locked by default. The
  operator must send `robot_lock: false` (via the A→B unlock sequence)
  before `/cmd_vel` will publish non-zero.

### Both controllers (local + remote) fighting each other

This is by design: local wins for 1 s after the last local packet, then
remote takes over automatically. If you don't want this, set
`cfg.local_dongle_enabled = False` or run with `--no-local-dongle`.

---

## Conventions in this codebase

A few patterns that recur and are worth knowing about:

- **`log(tag, msg)`** is used everywhere instead of `print`. It writes
  to stdout in `[tag] msg` format and flushes. journalctl picks it up.
- **`truthy(x)`** accepts bool / int / strings like `"true"`, `"yes"`,
  `"on"`, `"pressed"`. Operator-side fields use varied conventions so
  this is the safe parser.
- **`first_float(pkt, keys, default)`** returns the first parseable
  float from a list of candidate keys. Same for `first_int`. Lets the
  wire format vary slightly without breaking the receiver.
- **All subsystems are daemon threads with `.start()` and `.stop()`.**
  `stop()` is always idempotent and always safe to call during
  shutdown. If you add a new subsystem, follow this pattern.
- **State is protected by per-subsystem `threading.Lock`s**, not one
  global lock. Don't introduce cross-subsystem locks — it's a recipe
  for deadlock.
- **`rclpy.init()` is called exactly once**, by `teleop.main()`. Don't
  call it again from inside a subsystem.

---

## Whom to ask

- Original author: Aadi
- Operator script: `prod_revopilot_udp_highlowfreq_gamepad_cmds_win_lin.py`
  (lives in a separate repo on the operator PC)
- Daily.co account / API key: dashboard at https://dashboard.daily.co
- Point One Polaris account: https://app.pointonenav.com

If this README is wrong, fix it in the same PR as the code change that
made it wrong.