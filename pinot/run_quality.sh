#!/bin/bash
# Quality runs (all 22,458 test queries, concurrency 8) then the isolated latency pass for one table.
# Usage: run_quality.sh et|local
T=$1
cd $EVAL_HOME
L=results/sequential_run_log.md
PY=".venv/bin/python -I pinot_runs.py --table $T"
for m in bm25 bm25_global vector hybrid_sql hybrid_client hybrid_filter; do
  echo "- $(date '+%F %T') $T quality $m start" >> $L
  $PY --mode $m --concurrency 8 > results/logs/pinot_${T}_${m}.log 2>&1 || echo "- $(date '+%F %T') $T $m FAILED" >> $L
  echo "- $(date '+%F %T') $T quality $m done: $(grep -h '"name"' results/logs/pinot_${T}_${m}.log | tail -1 | cut -c1-200)" >> $L
done
echo "- $(date '+%F %T') $T latency pass start" >> $L
$PY --latency > results/logs/pinot_${T}_latency.log 2>&1 || echo "- $(date '+%F %T') $T latency FAILED" >> $L
echo "- $(date '+%F %T') $T latency pass done" >> $L
echo QUALITY-DONE
