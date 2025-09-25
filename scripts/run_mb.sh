#!/usr/bin/env bash
# scripts/run_mb.sh
# Launch LeoPar microbenchmarks across all ranks defined in cluster.ini.
# It passes a per-rank log_path argument IMMEDIATELY AFTER <rank>, as requested.
#
# Benches & expected argv (with log_path after rank):
#   mb_create_join   <cluster.ini> <rank> <log_path> <peer_rank> <iters> <mode>
#   mb_func_announce <cluster.ini> <rank> <log_path> <peer_rank> <count>
#   mb_scheduler     <cluster.ini> <rank> <log_path> <peer_rank> <iters> <repeat>
#
# Examples:
#   scripts/run_mb.sh --bench create_join --config cluster.ini --iters 10000 --mode remote \
#                     --ucx-home /usr/local/ucx-1.16.0 --ucx-tls "rc_x,ud_x,sm,self"
#   scripts/run_mb.sh --bench func_announce --config cluster.ini --count 32
#   scripts/run_mb.sh --bench scheduler --config cluster.ini --iters 5000 --repeat 3
set -euo pipefail

# -------- defaults --------
BENCH=""
CONFIG="cluster.ini"
UCX_HOME="/usr/local/ucx-1.16.0"
UCX_TLS="rc_x,ud_x,sm,self"
BIN_DIR="bin"
PROJECT_DIR="$(pwd -P)"
LOG_DIR="logs"          # will be created on each node
MODE="remote"          # for create_join: remote|local|auto
ITERS=10000            # for create_join / scheduler
COUNT=32               # for func_announce
REPEAT=1               # for scheduler
SSH_OPTS="-o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=5"

# -------- parse args --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bench)        BENCH="$2"; shift 2;;
    --config)       CONFIG="$2"; shift 2;;
    --ucx-home)     UCX_HOME="$2"; shift 2;;
    --ucx-tls)      UCX_TLS="$2"; shift 2;;
    --bin)          BIN_DIR="$2"; shift 2;;
    --project-dir)  PROJECT_DIR="$2"; shift 2;;
    --log-dir)      LOG_DIR="$2"; shift 2;;
    --mode)         MODE="$2"; shift 2;;
    --iters)        ITERS="$2"; shift 2;;
    --count)        COUNT="$2"; shift 2;;
    --repeat)       REPEAT="$2"; shift 2;;
    -h|--help)
      cat <<EOF
Usage:
  $0 --bench {create_join|func_announce|scheduler} --config cluster.ini [options]

Options:
  --ucx-home   /path/to/ucx           (default: /usr/local/ucx-1.16.0)
  --ucx-tls    "rc_x,ud_x,sm,self"    (or "tcp,sm,self")
  --bin        bin                    (directory of binaries)
  --project-dir <dir>                 (cd into this dir on remote before run)
  --log-dir    logs                   (per-rank runtime log file directory)
  --mode       remote|local|auto      (for create_join)
  --iters      10000                  (create_join/scheduler)
  --count      32                     (func_announce)
  --repeat     1                      (scheduler)
EOF
      exit 0;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

[[ -z "$BENCH" ]] && { echo "ERROR: --bench required"; exit 1; }
[[ -f "$CONFIG" ]] || { echo "ERROR: config not found: $CONFIG"; exit 1; }

case "$BENCH" in
  create_join)    BIN="${BIN_DIR}/mb_create_join" ;;
  func_announce)  BIN="${BIN_DIR}/mb_func_announce" ;;
  scheduler)      BIN="${BIN_DIR}/mb_scheduler" ;;
  *) echo "ERROR: unsupported bench: $BENCH"; exit 1;;
esac
[[ -x "$BIN" ]] || { echo "ERROR: binary not found/executable: $BIN"; exit 1; }

# -------- parse cluster.ini --------
WORLD_SIZE=$(grep -E '^world_size=' "$CONFIG" | head -n1 | cut -d= -f2)
[[ -n "$WORLD_SIZE" ]] || { echo "ERROR: world_size not found in $CONFIG"; exit 1; }

declare -a IPS
for (( r=0; r<WORLD_SIZE; r++ )); do
  ip=$(grep -E "^rank${r}=" "$CONFIG" | head -n1 | cut -d= -f2)
  [[ -n "$ip" ]] || { echo "ERROR: rank${r}=IP missing in $CONFIG"; exit 1; }
  IPS[$r]="$ip"
done

# -------- helpers --------
local_ips() { ip -o -4 addr show | awk '{print $4}' | cut -d/ -f1; }
is_local_ip() {
  local ip="$1"
  for lip in $(local_ips); do
    [[ "$ip" == "$lip" ]] && return 0
  done
  [[ "$ip" == "127.0.0.1" || "$ip" == "localhost" ]] && return 0
  return 1
}

build_cmd() {
  local rank="$1"
  local ip="${IPS[$rank]}"
  local peer=$(( (rank + 1) % WORLD_SIZE ))

  # runtime log path passed to the program (AFTER <rank>)
  local rt_log="${LOG_DIR}/leopar_rank${rank}-${ip}.log"

  # stdout/stderr of the process captured separately
  local run_log="${LOG_DIR}/${BENCH}_stdout_rank${rank}-${ip}.out"

  # prepare env and dir
  local pre="cd \"$PROJECT_DIR\"; mkdir -p \"$LOG_DIR\"; \
export LD_LIBRARY_PATH=\"${UCX_HOME}/lib:\$LD_LIBRARY_PATH\"; \
export UCX_HOME=\"$UCX_HOME\"; export UCX_TLS=\"$UCX_TLS\";"

  case "$BENCH" in
    create_join)
      # <cluster.ini> <rank> <log_path> <peer_rank> <iters> <mode>
      echo "$pre \"$BIN\" \"$CONFIG\" $rank \"$rt_log\" $peer $ITERS \"$MODE\" 2>&1 | tee \"$run_log\""
      ;;
    func_announce)
      # <cluster.ini> <rank> <log_path> <peer_rank> <count>
      echo "$pre \"$BIN\" \"$CONFIG\" $rank \"$rt_log\" $peer $COUNT 2>&1 | tee \"$run_log\""
      ;;
    scheduler)
      # <cluster.ini> <rank> <log_path> <peer_rank> <iters> <repeat>
      echo "$pre \"$BIN\" \"$CONFIG\" $rank \"$rt_log\" $peer $ITERS $REPEAT 2>&1 | tee \"$run_log\""
      ;;
  esac
}

echo "[run_mb] bench=$BENCH world_size=$WORLD_SIZE ucx_home=$UCX_HOME ucx_tls=\"$UCX_TLS\""
echo "[run_mb] bin=$BIN project_dir=$PROJECT_DIR config=$CONFIG log_dir=$LOG_DIR"

# -------- launch all ranks --------
declare -a PIDS
for (( r=0; r<WORLD_SIZE; r++ )); do
  ip="${IPS[$r]}"
  cmd="$(build_cmd "$r")"
  echo "[run_mb] start rank=$r ip=$ip"
  if is_local_ip "$ip"; then
    bash -lc "$cmd" &
    PIDS[$r]=$!
  else
    ssh $SSH_OPTS "$ip" "bash -lc '$cmd'" &
    PIDS[$r]=$!
  fi
done

# -------- wait ----------
FAIL=0
for (( r=0; r<WORLD_SIZE; r++ )); do
  if ! wait "${PIDS[$r]}"; then
    echo "[run_mb] rank $r failed"; FAIL=1
  fi
done
[[ $FAIL -eq 0 ]] && echo "[run_mb] all ranks completed." || { echo "[run_mb] completed with failures"; exit 1; }
