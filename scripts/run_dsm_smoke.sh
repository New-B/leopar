#!/bin/bash
# run_dsm_smoke.sh
# Auto-launch LeoPar UCX runtime across cluster nodes via ssh
# Usage:
#   ./scripts/run_dsm_smoke.sh conf/cluster.ini

CONFIG=$1  # get config file from the first argument
BIN=bin/dsm_smoke

if [ -z "$CONFIG" ]; then  # check the config file 
  echo "Usage: $0 <config_path>"
  exit 1
fi

if [ ! -f "$BIN" ]; then   # check the executable file
  echo "Error: binary $BIN not found, run 'make' first."
  exit 2
fi

# extrct the value of world_size from the config file
WORLD_SIZE=$(grep -i '^world_size' "$CONFIG" | cut -d'=' -f2 | tr -d ' ')
if [ -z "$WORLD_SIZE" ]; then
  echo "Error: world_size not found in $CONFIG"
  exit 3
fi

ROOT=$(pwd)
TS=$(date +%Y%m%d_%H%M%S)

echo "Launching LeoPar across $WORLD_SIZE nodes..."

HOST0=$(grep -i "^rank0" "$CONFIG" | cut -d'=' -f2 | tr -d ' ')
LOGFILE="logs/leopar_rank0_${HOST0}_${TS}.log"
LOGFILE_ERR="logs/error.log"

echo "  -> Starting rank 0 locally"
(
  cd "$ROOT" && mkdir -p logs && : > "$LOGFILE" && \
  #nohup "$BIN" "$CONFIG" 0 "$LOGFILE" >>"$LOGFILE" 2>&1 & 
  nohup "$BIN" "$CONFIG" 0 "$LOGFILE" >>"$LOGFILE" 2> "$LOGFILE_ERR" &
)

for (( RANK=1; RANK<$WORLD_SIZE; RANK++ )); do
  HOST=$(grep -i "^rank$RANK" "$CONFIG" | cut -d'=' -f2 | tr -d ' ') # get the IP address of each rank
  if [ -z "$HOST" ]; then
    echo "Error: IP for rank $RANK not found in $CONFIG"
    exit 4
  fi

  LOGFILE="logs/leopar_rank${RANK}_${HOST}_${TS}.log"

  echo "  -> Starting rank $RANK on $HOST"

ssh -n "$HOST" "cd $ROOT && mkdir -p logs && : > '$LOGFILE' && \
  nohup $BIN '$CONFIG' $RANK '$LOGFILE' >>'$LOGFILE' 2> "$LOGFILE_ERR" & "\
  || { echo "  !! SSH/launch failed on $HOST (rank $RANK)"; }
done

echo "All ranks launched.  Logs on each node under: logs/"
