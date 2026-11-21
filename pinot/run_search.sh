#!/bin/bash
# ES then OS, one at a time: rm stale container -> up -> index (+force-merge) -> timings -> quality modes
# -> latency pass (c=1,8; 1,000 queries after 200 warm-up; request_cache=false) -> timings -> down.
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
cd $E
state() { echo "swap $(sysctl -n vm.swapusage | awk '{print $6}'), VM RSS $(top -l 1 -o mem -stats command,mem | grep -m1 'com.apple.Virtua' | awk '{print $NF}')"; }
log() { echo "- $(date '+%F %T') $* [$(state)]" >> $L; }
PY=".venv/bin/python -I search_baselines.py"
for eng in "$@"; do
  c=$([ $eng = es ] && echo esci-es || echo esci-os)
  docker rm -f $c > /dev/null 2>&1
  log "$eng: removed stale container $c (if any); up with script defaults"
  $PY --engine $eng up > results/logs/${eng}_up.log 2>&1 || { log "$eng: UP FAILED"; continue; }
  log "$eng: up: $(tail -1 results/logs/${eng}_up.log | cut -c1-250)"
  log "$eng: index start"
  $PY --engine $eng index > results/logs/${eng}_index.log 2>&1 || { log "$eng: INDEX FAILED"; $PY --engine $eng down; continue; }
  log "$eng: index done: $(grep -h '"docs_indexed"\|"bulk_errors"\|"index_secs_total\|"segments\|"force_merge_secs"\|"store_bytes"' results/logs/${eng}_index.log | tr -d '\n ')"
  $PY --engine $eng timings > results/logs/${eng}_timings.log 2>&1
  for m in bm25 knn hybrid_native hybrid_client; do
    log "$eng quality $m start"
    $PY --engine $eng run --mode $m > results/logs/${eng}_${m}.log 2>&1 || log "$eng $m FAILED"
    log "$eng quality $m done: $(grep -h '"name"' results/logs/${eng}_${m}.log | tail -1 | cut -c1-200)"
  done
  rm -f results/${eng}_latency.json
  log "$eng latency pass start"
  $PY --engine $eng latency > results/logs/${eng}_latency.log 2>&1 || log "$eng latency FAILED"
  log "$eng latency pass done"
  $PY --engine $eng timings > results/logs/${eng}_timings.log 2>&1
  $PY --engine $eng down > /dev/null 2>&1
  log "$eng: down"
done
log "search ALL-DONE"
