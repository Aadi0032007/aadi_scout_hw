#!/usr/bin/env bash
set -euo pipefail

# ══════════════════════════════════════════════════════════════════════════
# ros_start.sh — legacy filename was in the systemd unit; no ROS anymore.
#
# Brings up the Segway CAN motion executor (aadi_segway_can_wrapper.py) and
# then runs LAB.teleop in the foreground. On Ctrl-C / SIGTERM / teleop exit,
# gracefully tears the wrapper back down (which soft-stops motion, disables
# chassis, and exits).
#
# Intended to be the ExecStart of aadi_ros_start_teleop.service.
# ══════════════════════════════════════════════════════════════════════════

# ── Paths ────────────────────────────────────────────────────────────────

# Where the CAN wrapper + libctrl / libcontrolcan .so files live
CAN_DIR="${CAN_DIR:-/home/revolabs/Revobots/Segway/CAN}"
WRAPPER="${WRAPPER:-${CAN_DIR}/aadi_segway_can_wrapper.py}"

# LAB codebase (teleop lives here)
LAB_DIR="${LAB_DIR:-/home/revolabs/aditya/aadi_scout_hw}"

# Ready signalling — wrapper touches this once chassis is green
READY_FILE="${READY_FILE:-/tmp/aadi_segway_can.ready}"

# Log for the wrapper (systemd captures teleop's stdout; wrapper's goes here)
WRAPPER_LOG="${WRAPPER_LOG:-/tmp/aadi_segway_can.log}"

# How long to wait for the ready file (seconds)
READY_TIMEOUT="${READY_TIMEOUT:-30}"

# UDP ports — must match cfg.docker_motion_port and cfg.battery_udp_port
MOTION_PORT="${MOTION_PORT:-56000}"
STATUS_PORT="${STATUS_PORT:-56500}"

# Python
PYTHON="${PYTHON:-/usr/bin/python3}"

# ── Tracking ─────────────────────────────────────────────────────────────

WRAPPER_PID=""
TELEOP_PID=""

# ── Logging helpers ──────────────────────────────────────────────────────

log()        { echo -e "\n[ros_teleop_start $(date '+%H:%M:%S')] $*\n"; }
log_inline() { echo   "[ros_teleop_start $(date '+%H:%M:%S')] $*"; }

# ── Cleanup: reverse of startup ──────────────────────────────────────────

cleanup() {
  # Stop teleop first so it doesn't keep sending UDP into a dying wrapper
  if [[ -n "$TELEOP_PID" ]] && kill -0 "$TELEOP_PID" 2>/dev/null; then
    log_inline "Stopping teleop.py (pid ${TELEOP_PID})..."
    kill -TERM "$TELEOP_PID" 2>/dev/null || true
    wait "$TELEOP_PID" 2>/dev/null || true
    TELEOP_PID=""
  fi

  # Stop wrapper — it soft-stops motion, disables chassis, unwinds SDK
  if [[ -n "$WRAPPER_PID" ]] && kill -0 "$WRAPPER_PID" 2>/dev/null; then
    log_inline "Stopping CAN wrapper (pid ${WRAPPER_PID})..."
    kill -TERM "$WRAPPER_PID" 2>/dev/null || true
    for _ in 1 2 3 4 5 6; do
      kill -0 "$WRAPPER_PID" 2>/dev/null || break
      sleep 0.5
    done
    if kill -0 "$WRAPPER_PID" 2>/dev/null; then
      log_inline "Wrapper did not exit cleanly — SIGKILL"
      kill -KILL "$WRAPPER_PID" 2>/dev/null || true
    fi
    WRAPPER_PID=""
  fi

  # Clean up the ready file if the wrapper crashed before removing it
  rm -f "$READY_FILE" 2>/dev/null || true

  log_inline "Done."
}
trap cleanup EXIT SIGINT SIGTERM

# ── Sanity checks ────────────────────────────────────────────────────────

if [[ ! -f "$WRAPPER" ]]; then
  echo "[ros_teleop_start] ERROR: wrapper not found: $WRAPPER" >&2
  exit 2
fi
if [[ ! -d "$LAB_DIR" ]]; then
  echo "[ros_teleop_start] ERROR: LAB_DIR not found: $LAB_DIR" >&2
  exit 2
fi

# Purge any stale ready file from a previous run — otherwise we'd think
# the new wrapper is ready before it's actually started.
rm -f "$READY_FILE" 2>/dev/null || true

# ── 1. Spawn CAN wrapper ─────────────────────────────────────────────────

log "Starting Segway CAN wrapper..."
log_inline "  ${WRAPPER}"
log_inline "  motion_port=${MOTION_PORT} status_port=${STATUS_PORT}"
log_inline "  ready_file=${READY_FILE}"
log_inline "  log=${WRAPPER_LOG}"

# Launch wrapper detached from our stdin, stdout to log file. It's owned
# by us via WRAPPER_PID so the trap can kill it cleanly.
"$PYTHON" "$WRAPPER" \
  --motion-port "$MOTION_PORT" \
  --status-port "$STATUS_PORT" \
  --ready-file "$READY_FILE" \
  > "$WRAPPER_LOG" 2>&1 &
WRAPPER_PID=$!
log_inline "Wrapper PID ${WRAPPER_PID}"

# ── 2. Wait for chassis to reach green ───────────────────────────────────

log "Waiting up to ${READY_TIMEOUT}s for chassis green..."
DEADLINE=$(( $(date +%s) + READY_TIMEOUT ))
while [[ ! -f "$READY_FILE" ]]; do
  # Wrapper crashed before signalling ready?
  if ! kill -0 "$WRAPPER_PID" 2>/dev/null; then
    echo "[ros_teleop_start] ERROR: wrapper exited before chassis ready" >&2
    echo "[ros_teleop_start] ---- last 40 lines of ${WRAPPER_LOG} ----" >&2
    tail -n 40 "$WRAPPER_LOG" >&2 || true
    exit 3
  fi
  if (( $(date +%s) > DEADLINE )); then
    echo "[ros_teleop_start] ERROR: chassis did not reach green within ${READY_TIMEOUT}s" >&2
    echo "[ros_teleop_start] ---- last 40 lines of ${WRAPPER_LOG} ----" >&2
    tail -n 40 "$WRAPPER_LOG" >&2 || true
    exit 3
  fi
  sleep 0.5
done
log "Chassis green — wrapper reports ready."

# ── 3. Start teleop in the foreground ────────────────────────────────────

log "Starting LAB/teleop.py..."
cd "$LAB_DIR"
"$PYTHON" -m LAB.teleop &
TELEOP_PID=$!
log_inline "teleop PID ${TELEOP_PID}"

# Wait on teleop. If teleop exits (SIGTERM, crash, etc), the EXIT trap
# fires and stops the wrapper.
wait "$TELEOP_PID"
RC=$?
TELEOP_PID=""

log_inline "teleop exited rc=${RC}"
exit "$RC"