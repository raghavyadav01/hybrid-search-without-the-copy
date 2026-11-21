#!/bin/bash
# Pinot local copy, sequential run: stop the ET cluster, fresh cluster, create esci_local, ingest from
# esci.products_us_pi (timed, with IdealState/ExternalView watch), collect timings, then all quality modes.
set -u
E=$EVAL_HOME
STP=$STARTREE_PINOT_HOME
L=$E/results/sequential_run_log.md
RUN=$E/stp-cache/run
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
cd $E
log() { echo "- $(date '+%F %T') $*" >> $L; }

# 1. stop ET cluster and clean its state
pid=$(cat $RUN/quickstart.pid)
if ps -o command= -p $pid 2>/dev/null | grep -q S3ParquetQuickstart; then kill $pid; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid; fi
sleep 2
mkdir -p $E/stp-cache/prev-logs/et-seq
gzip -c $RUN/quickstart.log > $E/stp-cache/prev-logs/et-seq/quickstart.log.gz 2>/dev/null &
gzpid=$!
docker exec esci-minio mc rm --recursive --force local/pinot-tier/segments > /dev/null 2>&1
wait $gzpid
rm -rf $RUN/tmp $RUN/local $RUN/quickstart.log
log "local: ET cluster stopped (pid $pid), state cleared, ET log archived (gz); swap $(sysctl -n vm.swapusage | awk '{print $6}')"

# 2. fresh cluster
bash $E/pinot/launch.sh > /dev/null
until [ "$(curl -s localhost:19000/instances | grep -o 'Server_' | wc -l)" -ge 3 ] && curl -s localhost:19000/instances | grep -q Minion; do sleep 2; done
log "local: fresh cluster up (pid $(cat $RUN/quickstart.pid))"
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_local.tableconfigs.json >> $L; echo >> $L

# 3. ingestion job (timed) with watcher
mkdir -p $RUN/local
nohup bash pinot/watch_table.sh esci_local_OFFLINE $RUN/local/watch.jsonl > /dev/null 2>&1 &
wpid=$!
T0=$(python3 -c 'import time;print(round(time.time(),2))')
log "local: ingestion job start t0=$T0 (UNDER SWAP PRESSURE: timings indicative)"
"$JAVA_HOME/bin/java" -Xmx4g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  -Daws.endpointUrlS3=http://127.0.0.1:9020 \
  -cp "$RUN/conf:$STP/startree-tools/target/classes:$(cat $E/stp-cache/startree-tools.full.cp)" \
  ai.startree.pinot.tools.admin.StarTreePinotAdministrator LaunchDataIngestionJob \
  -jobSpecFile $E/pinot/esci_local.ingestion-job.pi.yaml > $RUN/local/ingest.log 2>&1
rc=$?
T1=$(python3 -c 'import time;print(round(time.time(),2))')
log "local: ingestion job exit rc=$rc wall=$(python3 -c "print(round($T1-$T0,1))") s"
[ $rc -ne 0 ] && { log "local: INGESTION FAILED, see stp-cache/run/local/ingest.log"; kill $wpid; exit 1; }
until python3 - "$RUN/local/watch.jsonl" <<'PY'
import json,sys
r=[json.loads(x) for x in open(sys.argv[1]) if x.strip()]
sys.exit(0 if r and r[-1]["is"]>=33 and r[-1]["ev_online"]>=r[-1]["is"] else 1)
PY
do sleep 2; done
sleep 4; kill $wpid
log "local: all segments ONLINE"
.venv/bin/python -I collect_timings_seq.py --table local --log $RUN/quickstart.log --watch $RUN/local/watch.jsonl \
  --job-log $RUN/local/ingest.log --t0 $T0 --job-end $T1 > results/logs/collect_timings_local.log 2>&1
log "local: timings -> results/pinot_local_timings.json (measured under swap pressure)"

# 4. quality modes
bash pinot/run_modes.sh local bm25 bm25_global vector hybrid_sql hybrid_client hybrid_filter
sw0=$(sysctl -n vm.swapusage | awk '{print $6}' | tr -d M)
log "local latency pass start (swap ${sw0}M)"
.venv/bin/python -I pinot_runs.py --table local --latency > results/logs/pinot_local_latency.log 2>&1 || log "local latency FAILED"
sw1=$(sysctl -n vm.swapusage | awk '{print $6}' | tr -d M)
log "local latency pass done (swap ${sw1}M, delta $(python3 -c "print(round(($sw1-$sw0)/1024,2))") GB$(python3 -c "print(' -> FLAG: latency measured under swap growth' if $sw1-$sw0>5120 else '')"))"
bash pinot/run_modes.sh local hybrid_sql_perseg
echo LOCAL-DONE
