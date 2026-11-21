#!/bin/bash
# run_modes.sh <et|local> <mode...> : quality runs (all test queries, c=8), logged to the run log
# SAMPLE=N (env): run on the deterministic N-query sample (seed 2026); outputs get an _sN suffix
T=$1; shift
SA=""; SX=""; [ -n "$SAMPLE" ] && { SA="--sample $SAMPLE"; SX="_s$SAMPLE"; }
cd $EVAL_HOME
L=results/sequential_run_log.md
for m in "$@"; do
  echo "- $(date '+%F %T') $T quality $m$SX start (swap $(sysctl -n vm.swapusage | awk '{print $6}'))" >> $L
  .venv/bin/python -I pinot_runs.py --table $T --mode $m --concurrency 8 $SA > results/logs/pinot_${T}_${m}${SX}.log 2>&1 || echo "- $(date '+%F %T') $T $m$SX FAILED" >> $L
  echo "- $(date '+%F %T') $T quality $m$SX done: $(grep -h '"name"' results/logs/pinot_${T}_${m}${SX}.log | tail -1 | cut -c1-200)" >> $L
done
echo "- $(date '+%F %T') $T run_modes ALL-DONE" >> $L
