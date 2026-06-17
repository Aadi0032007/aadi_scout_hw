#!/usr/bin/env bash
set -euo pipefail

# ══════════════════════════════════════════════════════════════════════════
# ros_teleop_start.sh
#
# Brings up the segway_ros1 Docker stack (roscore, SmartCar, UDP keepalive,
# chassis enable), then runs teleop.py in the foreground. On Ctrl-C, SIGTERM
# (systemd stop), or teleop.py exiting on its own, gracefully tears the
# stack back down: disable chassis -> kill ROS nodes -> stop container.
#
# Intended to be the ExecStart of aadi_ros_start_teleop.service.
# ══════════════════════════════════════════════════════════════════════════

CONTAINER="segway_ros1"

# Paths INSIDE the container
ROS_SETUP="/opt/ros/noetic/setup.bash"
WS_SETUP="/root/catkin_ws/devel/setup.bash"
VENV_ACT="/root/catkin_ws/venv/bin/activate"
PY_NODE="/root/catkin_ws/revo_docker_udp_motion_keepalive.py"

# teleop.py location on the HOST
LAB_DIR="/home/revolabs/aditya/aadi_scout_hw/LAB"

# Serial config
TTY_DEV=""
TTY_BAUD="921600"
SMARTCAR_SERIAL=""   # derived from TTY_DEV once detected

# Poll interval while waiting for the Segway base at startup
POLL_SEC="1"

# ── logging ──────────────────────────────────────────────────────────────

log() { echo -e "\n[ros_teleop_start $(date '+%H:%M:%S')] $*\n"; }
log_inline() { echo "[ros_teleop_start $(date '+%H:%M:%S')] $*"; }

# ── helpers ──────────────────────────────────────────────────────────────

detect_tty_dev() {
  for dev in /dev/ttyUSB0 /dev/ttyACM0 /dev/rpserialport; do
    if [[ -e "$dev" ]]; then
      TTY_DEV="$dev"
      return 0
    fi
  done
  TTY_DEV=""
  return 1
}

container_exists() {
  docker inspect "$CONTAINER" >/dev/null 2>&1
}

container_running() {
  docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"
}

# Run a command inside the container in a login shell
dex() {
  docker exec -i "$CONTAINER" bash -lc "source '$ROS_SETUP' && source '$WS_SETUP' && $*"
}

# Start a long-running command inside the container in the background (detached)
dexec_bg() {
  log_inline "Launching in background: $*"
  docker exec -d "$CONTAINER" bash -lc "
    set -e
    source '$ROS_SETUP'
    source '$WS_SETUP'
    mkdir -p /root
    setsid nohup $* >> /root/segway_stack.log 2>&1 </dev/null &
  "
}

roscore_up() {
  container_running && dex "timeout 1s rosparam list >/dev/null 2>&1"
}

smartcar_up() {
  container_running && dex "timeout 1s rosservice list 2>/dev/null | grep -q '^/ros_set_chassis_enable_cmd_srv$'"
}

keepalive_up() {
  container_running && dex "ps aux | grep -F \"python3 ${PY_NODE}\" | grep -v grep >/dev/null 2>&1"
}

# ── serial setup (host stty + container symlink) ───────────────────────────

configure_serial() {
  local host_dev="$1"
  local container_dev="/dev/$(basename "$host_dev")"

  log "Configuring serial: ${container_dev} (inside container, runs as root — no sudo needed)"

  log_inline "Symlinking ${container_dev} -> /dev/rpserialport inside container..."
  docker exec -i "$CONTAINER" bash -lc "
    stty -F '${container_dev}' '${TTY_BAUD}' raw -echo
    ln -sf '${container_dev}' /dev/rpserialport
    ls -la '${container_dev}' /dev/rpserialport
  "
  log_inline "Serial ready: host=${host_dev}, container=${container_dev}, symlink=/dev/rpserialport"
}

# ── wait helpers ─────────────────────────────────────────────────────────

wait_for_serial() {
  log "Waiting for Segway serial device (ttyUSB0 / ttyACM0 / rpserialport)..."
  local i=0
  while ! detect_tty_dev; do
    i=$((i + 1))
    if (( i % 10 == 0 )); then
      log_inline "Still waiting for base... turn on Segway / plug in USB serial (${i}s)"
    fi
    sleep "$POLL_SEC"
  done
  log "Serial device found: ${TTY_DEV}"
}

wait_roscore() {
  log "Waiting for roscore..."
  for i in {1..100}; do
    if roscore_up; then
      log "roscore is up."
      return 0
    fi
    if (( i % 10 == 0 )); then
      log_inline "Still waiting for roscore... (${i}/100)"
    fi
    sleep 0.2
  done
  echo "[ros_teleop_start] ERROR: roscore did not come up." >&2
  return 1
}

wait_enable_service() {
  local svc="/ros_set_chassis_enable_cmd_srv"
  log "Waiting for service ${svc}..."
  for i in {1..200}; do
    if smartcar_up; then
      log "Enable service is available."
      return 0
    fi
    if (( i % 20 == 0 )); then
      log_inline "Still waiting for SmartCar / enable service... (${i}/200)"
    fi
    sleep 0.2
  done
  echo "[ros_teleop_start] ERROR: Enable service did not appear: ${svc}" >&2
  return 1
}

# ── stack bringup ────────────────────────────────────────────────────────

start_stack() {
  wait_for_serial
  SMARTCAR_SERIAL="$(basename "$TTY_DEV")"

  log "Starting container (if not already running)..."
  if ! container_exists; then
    echo "[ros_teleop_start] ERROR: Docker container does not exist: $CONTAINER" >&2
    return 1
  fi

  if ! container_running; then
    log_inline "Starting docker container ${CONTAINER}..."
    docker start "$CONTAINER" >/dev/null
    log_inline "Container started."
  else
    log "Container already running."
  fi

  configure_serial "$TTY_DEV"

  log_inline "Resetting /root/segway_stack.log"
  docker exec -i "$CONTAINER" bash -lc "mkdir -p /root && : > /root/segway_stack.log" >/dev/null 2>&1 || true

  log "Starting roscore (inside container)..."
  if roscore_up; then
    log "roscore already healthy."
  else
    dexec_bg "roscore --port 11311"
  fi
  wait_roscore

  log "Starting SmartCar node (inside container)..."
  if smartcar_up; then
    log "SmartCar already running."
  else
    dexec_bg "rosrun segwayrmp SmartCar _segwaySmartCarSerial:=${SMARTCAR_SERIAL}"
  fi

  log "Starting UDP->/cmd_vel keepalive python node (inside container)..."
  if keepalive_up; then
    log "Python keepalive already running."
  else
    log_inline "Launching python keepalive: ${PY_NODE}"
    docker exec -d "$CONTAINER" bash -lc "
      set -e
      source '$ROS_SETUP'
      source '$WS_SETUP'
      source '$VENV_ACT'
      mkdir -p /root
      setsid nohup python3 '$PY_NODE' >> /root/segway_stack.log 2>&1 </dev/null &
    "
    log_inline "Python keepalive launched."
  fi

  wait_enable_service

  log "Enabling chassis (inside container)..."
  local enable_result
  enable_result="$(dex "rosservice call /ros_set_chassis_enable_cmd_srv \"ros_set_chassis_enable_cmd: true\"" 2>&1 || true)"
  log_inline "Chassis enable result: ${enable_result}"

  log "Stack is up."
  return 0
}

# ── graceful shutdown ────────────────────────────────────────────────────

stop_stack() {
  log "Shutting down Segway ROS1 stack..."

  if ! container_running; then
    log "Container not running. Nothing to shut down."
    return 0
  fi

  log_inline "Disabling chassis (safe stop)..."
  docker exec -i "$CONTAINER" bash -lc "
    source '$ROS_SETUP' &&
    source '$WS_SETUP' &&
    rosservice call /ros_set_chassis_enable_cmd_srv 'ros_set_chassis_enable_cmd: false'
  " >/dev/null 2>&1 || true
  sleep 0.5

  log_inline "Stopping keepalive..."
  docker exec -i "$CONTAINER" bash -lc "
    pkill -f revo_docker_udp_motion_keepalive.py || true
  " >/dev/null 2>&1 || true

  log_inline "Stopping SmartCar..."
  docker exec -i "$CONTAINER" bash -lc "
    pkill -f SmartCar || true
  " >/dev/null 2>&1 || true

  log_inline "Stopping roscore..."
  docker exec -i "$CONTAINER" bash -lc "
    pkill -f /opt/ros/noetic/bin/roscore || true
    pkill -f roscore || true
  " >/dev/null 2>&1 || true
  sleep 0.5

  log_inline "Stopping docker container ${CONTAINER}..."
  docker stop "$CONTAINER" >/dev/null 2>&1 || true

  log "Shutdown complete."
}

# teleop.py's own PID, tracked so cleanup() can stop it if the trap fires
# while teleop is still running (e.g. SIGTERM from systemd).
TELEOP_PID=""

cleanup() {
  echo ""
  log_inline "Caught signal / exiting, shutting down..."

  if [[ -n "$TELEOP_PID" ]] && kill -0 "$TELEOP_PID" 2>/dev/null; then
    log_inline "Stopping teleop.py (pid ${TELEOP_PID})..."
    kill -TERM "$TELEOP_PID" 2>/dev/null || true
    wait "$TELEOP_PID" 2>/dev/null || true
  fi

  stop_stack
  log_inline "Done."
}
trap cleanup EXIT SIGINT SIGTERM

# ── main ─────────────────────────────────────────────────────────────────

log "Bringing up Segway ROS1 stack..."
start_stack

log "Starting LAB/teleop.py..."
cd "$LAB_DIR"

# No 'exec' here: keeping this as a regular foreground child means the
# EXIT trap above still fires (and runs stop_stack) when teleop.py exits
# or when systemd sends SIGTERM to this script.
python3 teleop.py &
TELEOP_PID=$!
wait "$TELEOP_PID"
TELEOP_PID=""