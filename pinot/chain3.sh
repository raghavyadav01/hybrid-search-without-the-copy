#!/bin/bash
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
stop_cluster() { pid=$(cat $E/stp-cache/run/quickstart.pid); if ps -o command= -p $pid 2>/dev/null | grep -q S3ParquetQuickstart; then kill $pid; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid; fi; echo "- $(date '+%F %T') pinot cluster stopped (pid $pid)" >> $L; }
clean() { mkdir -p $E/stp-cache/prev-logs/$1; grep -v "QueryLogger" $E/stp-cache/run/quickstart.log | head -c 300000000 > $E/stp-cache/prev-logs/$1/quickstart.log; rm -rf $E/stp-cache/run/tmp $E/stp-cache/run/quickstart.log; docker exec esci-minio mc rm --recursive --force local/pinot-tier/segments > /dev/null 2>&1; echo "- $(date '+%F %T') cleaned cluster state ($1)" >> $L; }
bash $E/pinot/run_local.sh > $E/results/logs/run_local.out 2>&1
stop_cluster; clean local-seq
bash $E/pinot/run_et_rest.sh > $E/results/logs/run_et_rest.out 2>&1
stop_cluster; clean et-seq2
echo "- $(date '+%F %T') CHAIN3 ALL-DONE" >> $L
