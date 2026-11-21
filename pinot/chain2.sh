#!/bin/bash
# Order (user): ES -> OS -> Pinot local -> ET remainder.
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
stop_cluster() { pid=$(cat $E/stp-cache/run/quickstart.pid); ps -o command= -p $pid 2>/dev/null | grep -q S3ParquetQuickstart && { kill $pid 2>/dev/null; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid; }; echo "- $(date '+%F %T') pinot cluster stopped (pid $pid)" >> $L; }
until grep -q "search ALL-DONE" $L; do sleep 30; done
bash $E/pinot/run_local.sh > $E/results/logs/run_local.out 2>&1
stop_cluster
rm -rf $E/stp-cache/run/tmp $E/stp-cache/run/quickstart.log
bash $E/pinot/run_et_rest.sh > $E/results/logs/run_et_rest.out 2>&1
stop_cluster
echo "- $(date '+%F %T') CHAIN2 ALL-DONE" >> $L
