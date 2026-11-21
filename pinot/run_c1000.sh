#!/bin/bash
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
cd $E
state() { echo "swap $(sysctl -n vm.swapusage | awk '{print $6}'), VM RSS $(top -l 1 -o mem -stats command,mem | grep -m1 'com.apple.Virtua' | awk '{print $NF}')"; }
log() { echo "- $(date '+%F %T') $* [$(state)]" >> $L; }
for eng in es os; do
  P() { .venv/bin/python -I search_baselines.py --engine $eng --num-cand 1000 "$@"; }
  P up > results/logs/${eng}_up_c1000.log 2>&1 || { log "$eng c1000: UP FAILED"; continue; }
  docs=$(curl -s localhost:$([ $eng = es ] && echo 9250 || echo 9260)/esci_products/_count | python3 -c "import json,sys; print(json.load(sys.stdin).get('count'))")
  log "$eng c1000: up (existing container + volume reused), esci_products docs=$docs"
  [ "$docs" = "1215854" ] || { log "$eng c1000: index NOT intact, skipping"; P down; continue; }
  for m in knn hybrid_native hybrid_client; do
    P run --mode $m > results/logs/${eng}_${m}_c1000.log 2>&1 || log "$eng c1000 $m FAILED"
    log "$eng c1000 quality $m done: $(grep -h '"name"' results/logs/${eng}_${m}_c1000.log | tail -1 | cut -c1-200)"
  done
  rm -f results/${eng}_latency_c1000.json
  log "$eng c1000 latency pass start"
  P latency --lat-modes knn,hybrid_native,hybrid_client > results/logs/${eng}_latency_c1000.log 2>&1 || log "$eng c1000 latency FAILED"
  log "$eng c1000 latency pass done"
  P down > /dev/null 2>&1
  log "$eng c1000: down"
done
log "c1000 ALL-DONE"
