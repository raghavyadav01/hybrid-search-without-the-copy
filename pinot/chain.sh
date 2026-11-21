#!/bin/bash
E=$EVAL_HOME
until grep -q "et run_modes ALL-DONE" $E/results/sequential_run_log.md; do sleep 20; done
bash $E/pinot/run_local.sh > $E/results/logs/run_local.out 2>&1
grep -q "local run_modes ALL-DONE" $E/results/sequential_run_log.md || exit 1
# Pinot local cluster must be down before ES
pid=$(cat $E/stp-cache/run/quickstart.pid); kill $pid; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid
echo "- $(date '+%F %T') local cluster stopped (pid $pid)" >> $E/results/sequential_run_log.md
bash $E/pinot/run_search.sh es os
