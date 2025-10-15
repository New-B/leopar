#!/usr/bin/env bash
# Sweep launcher for bin/matrix_multiply across node sizes, threads, and matrix sizes
# Args to binary: <config_path> <rank> <M> <threads_per_node> [log_path]

set -Eeuo pipefail

# ======== 用户可按需修改的配置 ========
# 需要遍历的节点规模（与 conf/cluster*.ini 对应）
NODE_SCALES=(2 4 8 16)

# 每个节点上的线程规模
THREADS_PER_NODE_SET=(2 4 8 16 28)

# 计算的方阵规模
M_SIZES=(1024 2048 4096)

# 配置文件前缀与目录
CONF_DIR="conf"
CONF_PREFIX="cluster"   # 例如 cluster2.ini、cluster4.ini ...

# 可执行文件路径（在远端节点也应相同路径可访问）
BIN="./bin/matrix_multiply"

# 远端工作目录（如果使用 NFS/相同路径，保持为当前项目根）
REMOTE_WORKDIR="$PWD"

# SSH 用户（如需要：ubuntu@），否则留空
USER_AT=""

# # SSH 额外选项
# SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8"

# 并发启动时每轮等待多少秒再检查（用于“流控”，无需太小）
WAIT_POLL_SEC=1

# 日志目录（本机保存汇总日志；远端节点各自写入单独 rank 日志文件）
LOG_ROOT="./logs_mm"
mkdir -p "$LOG_ROOT"
# =====================================

ts() { date +"%Y%m%d_%H%M%S"; }

# 从 conf/clusterN.ini 中解析 rank0..rank{N-1} 的 IP
parse_ips_from_conf() {
  local conf_file="$1"
  local need_n="$2"

  if [[ ! -f "$conf_file" ]]; then
    echo "Config file not found: $conf_file" >&2
    return 2
  fi

  awk -v N="$need_n" '
    BEGIN{for(i=0;i<N;i++) seen[i]=0;}
    $0 ~ /^rank[0-9]+=[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/ {
      split($0, a, "=");
      key=a[1]; ip=a[2];
      sub(/^rank/, "", key);
      r=int(key);
      if (r>=0 && r<N) { hosts[r]=ip; seen[r]=1; }
    }
    END{
      for(i=0;i<N;i++){
        if(!seen[i]) { printf("MISSING_RANK_%d\n", i); exit 1; }
        else { print hosts[i]; }
      }
    }
  ' "$conf_file"
}

# 在一个规模下运行一轮 sweep
run_one_nodesize() {
  local nodes="$1" threads="$2" msize="$3"
  local conf="${CONF_DIR}/${CONF_PREFIX}${nodes}.ini"

  # 解析 IP 列表
  mapfile -t HOSTS < <(parse_ips_from_conf "$conf" "$nodes") || {
    echo "[ERROR] failed to parse IPs from $conf (nodes=$nodes)" >&2
    return 3
  }

  # 报错保护
  for h in "${HOSTS[@]}"; do
    if [[ "$h" == MISSING_RANK_* ]]; then
      echo "[ERROR] $conf: missing ${h#MISSING_}" >&2
      return 4
    fi
  done

  local tag="N${nodes}_T${threads}_M${msize}"
  local round_dir="${LOG_ROOT}/$(ts)_${tag}"
  mkdir -p "$round_dir"

  echo "------------------------------------------------------------------"
  echo "[INFO] Starting round: nodes=${nodes}, threads_per_node=${threads}, M=${msize}"
  echo "[INFO] Conf: $conf"
  echo "[INFO] Logs: $round_dir"
  echo "------------------------------------------------------------------"

  # 在每个 rank 上启动
  local pids=()
  local rank=0
  for host in "${HOSTS[@]}"; do
    local log_remote="${round_dir}/${tag}_rank${rank}.log"
    # 要传给可执行文件的 log_path（可执行文件最后一个 [log_path]）
    local log_arg="$log_remote"

    # 远端命令：进入工作目录、可选导出 OMP、执行二进制
    # 注意：把所有参数都显式传递，避免路径带空格产生问题
    {
      ssh "${host}" "bash -lc 'cd \"$REMOTE_WORKDIR\" && \
        mkdir -p \"$(dirname "$log_remote")\" && \
        { \
          export OMP_NUM_THREADS=$threads || true; \
          \"$BIN\" \"$conf\" $rank $msize $threads \"$log_arg\" \
            >\"$log_remote.out\" 2>\"$log_remote.err\"; \
        }'" &
    } || true
    pids+=($!)
    printf "[LAUNCH] host=%-15s rank=%-2d -> %s\n" "$host" "$rank" "$log_remote"
    ((rank++))
  done

  # 等所有 rank 结束
  local fail=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then fail=1; fi
    sleep "$WAIT_POLL_SEC"
  done

  # 汇总结果（把每个 rank 的 .out/.err 合并到一个清单）
  {
    echo "=== SUMMARY $tag ==="
    for ((r=0; r<nodes; r++)); do
      local lf="${round_dir}/${tag}_rank${r}.log"
      echo "-- rank $r --"
      [[ -s "${lf}.out" ]] && { echo "[STDOUT]"; tail -n +1 "${lf}.out"; } || echo "[STDOUT empty]"
      [[ -s "${lf}.err" ]] && { echo "[STDERR]"; tail -n +1 "${lf}.err"; } || echo "[STDERR empty]"
    done
  } > "${round_dir}/SUMMARY_${tag}.txt"

  if (( fail )); then
    echo "[WARN] Round $tag finished with failures. See ${round_dir}/ for details."
    return 1
  else
    echo "[OK] Round $tag finished successfully."
  fi
}

main() {
  # 预检
  if [[ ! -x "$BIN" ]]; then
    echo "[ERROR] binary not found or not executable: $BIN" >&2
    exit 10
  fi

  # 三层循环：节点规模 -> 线程规模 -> 矩阵规模
  for nodes in "${NODE_SCALES[@]}"; do
    conf="${CONF_DIR}/${CONF_PREFIX}${nodes}.ini"
    if [[ ! -f "$conf" ]]; then
      echo "[WARN] Missing conf for nodes=$nodes: $conf (skipped)"; continue
    fi
    for threads in "${THREADS_PER_NODE_SET[@]}"; do
      for m in "${M_SIZES[@]}"; do
        # 单轮运行；失败不终止全局 sweep，继续下一组合
        if ! run_one_nodesize "$nodes" "$threads" "$m"; then
          echo "[WARN] Combination failed: nodes=$nodes threads=$threads M=$m"
        fi
      done
    done
  done

  echo
  echo "[DONE] Sweep finished. All logs under: $LOG_ROOT"
}

main "$@"
