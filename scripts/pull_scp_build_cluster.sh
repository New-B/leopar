#!/bin/bash
# scripts/pull_scp_build_cluster.sh
# 从本地发起：让所有节点 clone/pull 指定仓库并编译
# 用法：
#   ./scripts/pull_scp_build_cluster.sh conf/cluster.ini


CONFIG=$1
REF=${2:-main}
REMOTE_ROOT=${3:-$(pwd)}
REPO_URL=${4:-https://github.com/New-B/leopar.git}

if [ -z "$CONFIG" ]; then
  echo "Usage: $0 <config_path>"
  exit 1
fi

WORLD_SIZE=$(grep -i '^world_size' "$CONFIG" | cut -d'=' -f2 | tr -d ' ')
if [ -z "$WORLD_SIZE" ]; then
  echo "Error: world_size not found in $CONFIG"
  exit 2
fi

echo "[INFO] Repo     : $REPO_URL"
echo "[INFO] Ref      : $REF"
echo "[INFO] RemoteDir: $REMOTE_ROOT"
echo "[INFO] WorldSize: $WORLD_SIZE"
echo

echo "[LOCAL] make clean ..."
make -C "$REMOTE_ROOT" clean || true
if git -C "$REMOTE_ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[LOCAL] git fetch/checkout/pull ..."
  git -C "$REMOTE_ROOT" fetch --all --tags --prune
  git -C "$REMOTE_ROOT" checkout "$REF" || true
  git -C "$REMOTE_ROOT" pull --ff-only origin "$REF" || true
else
  echo "[LOCAL] Not a git repo under $REMOTE_ROOT, skip git update."
fi

for (( RANK=1; RANK<$WORLD_SIZE; RANK++ )); do
  HOST=$(grep -i "^rank$RANK" "$CONFIG" | cut -d'=' -f2 | tr -d ' ')
  if [ -z "$HOST" ]; then
    echo "Error: IP for rank $RANK not found in $CONFIG"
    exit 3
  fi

  echo "==> [$HOST] rank $RANK : mkdir and scp whole directory"
  ssh -n "$HOST" "mkdir -p '$REMOTE_ROOT'"
  scp -rpC "$REMOTE_ROOT"/. "$HOST:$REMOTE_ROOT"

  echo "==> [$HOST] rank $RANK : make"
  ssh -n "$HOST" "cd '$REMOTE_ROOT' && (make clean || true) && make"
  echo "==> [$HOST] done."
  echo
done

echo "[LOCAL] make ..."
make -C "$REMOTE_ROOT"

echo "[ALL DONE]"
