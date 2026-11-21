#!/bin/bash
# chain5 (2026-10-25, after the disk-full incident): ET remainder -> freshness (ET, ES, OS, local) -> storage
# -> restart paused containers -> table1.md / timings.md.
# Every pass runs under a disk guard: free space on / < 15 GB -> the pass is killed, logged, the Pinot
# cluster is stopped and the chain exits (nothing downstream runs on a full disk).
# Local leftovers lost to the disk-full at 07:45 (hybrid_filter c8 latency, hybrid_sql_perseg quality run)
# are folded into the freshness local cluster (same esci_local base ingest, before any append).
set -m   # background jobs get their own process group, so the guard can kill the whole pass
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
RUN=$E/stp-cache/run
cd $E
PY=".venv/bin/python -I"
MIN_FREE_GB=15
sw() { sysctl -n vm.swapusage | awk '{print $6}'; }
free_gb() { df -g / | awk 'NR==2{print $4}'; }
log() { echo "- $(date '+%F %T') c5: $* [swap $(sw), free $(free_gb)G]" >> $L; }
logsz() { [ -f $RUN/quickstart.log ] && stat -f %z $RUN/quickstart.log || echo 0; }

stop_cluster() {
  pid=$(cat $RUN/quickstart.pid 2>/dev/null)
  if [ -n "$pid" ] && ps -o command= -p $pid 2>/dev/null | grep -q S3ParquetQuickstart; then
    kill $pid; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid
  fi
  rm -rf $RUN/tmp $RUN/quickstart.log
  docker exec esci-minio mc rm --recursive --force local/pinot-tier/segments > /dev/null 2>&1
  log "cluster stopped + cleaned (pid $pid)"
}
abort() { log "CHAIN5 ABORTED: $*"; stop_cluster; exit 1; }

# g NAME CMD... : run CMD as its own process group; kill it if free disk < MIN_FREE_GB.
g() {
  local name=$1; shift
  "$@" & local p=$!
  while kill -0 $p 2>/dev/null; do
    if [ "$(free_gb)" -lt $MIN_FREE_GB ]; then
      kill -TERM -- -$p 2>/dev/null; sleep 5; kill -KILL -- -$p 2>/dev/null
      log "DISK GUARD: '$name' stopped (free < ${MIN_FREE_GB}G)"; wait $p 2>/dev/null
      abort "disk guard tripped in '$name'"
    fi
    sleep 10
  done
  wait $p
}

up_cluster() {
  bash pinot/launch.sh > /dev/null
  until [ "$(curl -s localhost:19000/instances | grep -o 'Server_' | wc -l)" -ge 3 ] && curl -s localhost:19000/instances | grep -q Minion; do sleep 2; done
}
wait_online() { until [ "$(curl -s "localhost:19000/tables/$1/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; print(sum(1 for m in d.values() if "ONLINE" in m.values()))
except Exception: print(0)')" -ge 33 ]; do sleep 5; done; }

# 0. Docker (Docker Desktop's engine died at 07:26 when the disk filled): wait for it, start MinIO + REST.
until docker info > /dev/null 2>&1; do sleep 15; done
docker start esci-minio esci-iceberg-rest > /dev/null 2>&1
until curl -s -m 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:9020/minio/health/live | grep -q 200 \
   && curl -s -m 5 http://127.0.0.1:8190/v1/config > /dev/null; do sleep 5; done
[ "$(free_gb)" -lt $MIN_FREE_GB ] && { log "CHAIN5 not started: free < ${MIN_FREE_GB}G"; exit 1; }
log "start (docker + MinIO + REST up); log4j2-eval.xml active"
stop_cluster   # stale tmp / tier segments from the killed chain3

# 1. ET remainder: clean onboarding (timed), cold probe, hybrid_filter, hybrid_sql_perseg, latency
up_cluster; log "et: fresh cluster up (pid $(cat $RUN/quickstart.pid))"
mkdir -p $RUN/et; rm -f $RUN/et/watch.jsonl
nohup bash pinot/watch_table.sh esci_et_OFFLINE $RUN/et/watch.jsonl > /dev/null 2>&1 &
wpid=$!
T0=$(python3 -c 'import time;print(round(time.time(),2))')
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_et.pi.tableconfigs.json >> $L; echo >> $L
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_et_OFFLINE" > /dev/null
log "et: esci_et created + sync scheduled t0=$T0"
g et-onboard bash -c 'until python3 - "$0" <<"PY"
import json,sys
r=[json.loads(x) for x in open(sys.argv[1]) if x.strip()]
sys.exit(0 if r and r[-1]["is"]>=33 and r[-1]["ev_online"]>=33 else 1)
PY
do sleep 2; done' $RUN/et/watch.jsonl
sleep 4; kill $wpid
log "et: 33/33 ONLINE; quickstart.log $(( $(logsz) / 1048576 )) MB"
# log check: every line collect_timings*.py parses must still be logged under log4j2-eval.xml
miss=""; counts=""
for p in "esci_et_OFFLINE_controller_" "Generating segment:" "Uploaded segment:" "Added 33 segments" \
         "Adding new ONLINE segment" "Creating new text index" "Created text index" "Creating new Vector index" \
         "Created Vector index" "from local indexDir" "Added new immutable segment"; do
  c=$(grep -c -F "$p" $RUN/quickstart.log); counts="$counts '$p'=$c"; [ "$c" -eq 0 ] && miss="$miss '$p'"
done
log "et logcheck:$counts"
[ -n "$miss" ] && log "et logcheck MISSING:$miss (timings incomplete; fix log4j2-eval.xml)"
g et-timings $PY collect_timings_seq.py --table et --log $RUN/quickstart.log --watch $RUN/et/watch.jsonl --t0 $T0 \
  > results/logs/collect_timings_et.log 2>&1 || log "et: collect_timings FAILED"
log "et: timings -> results/pinot_et_timings.json (clean rerun)"
g et-cold $PY pinot_runs.py --table et --cold > results/logs/pinot_et_cold.log 2>&1 || log "et: cold probe FAILED"
log "et: cold probe done -> results/pinot_et_cold.json"
for m in hybrid_filter hybrid_sql_perseg; do
  s0=$(logsz); g et-$m bash pinot/run_modes.sh et $m
  log "et: $m quickstart.log +$(( ($(logsz) - s0) / 1048576 )) MB"
done
s0=$(logsz); w0=$(sysctl -n vm.swapusage | awk '{print $6}' | tr -d M)
log "et latency pass start"
g et-latency $PY pinot_runs.py --table et --latency > results/logs/pinot_et_latency.log 2>&1 || log "et latency FAILED"
w1=$(sysctl -n vm.swapusage | awk '{print $6}' | tr -d M)
log "et latency pass done (swap delta $(python3 -c "print(round(($w1-$w0)/1024,2))") GB; quickstart.log +$(( ($(logsz) - s0) / 1048576 )) MB)"
stop_cluster
log "ET-REMAINDER DONE"

# 2. freshness: ET, ES, OS, local (3 runs each, median in freshness.json); snapshot restored after each
$PY freshness.py rollback >> results/logs/freshness.log 2>&1
log "fresh: ET start"
up_cluster
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_et.pi.tableconfigs.json > /dev/null
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_et_OFFLINE" > /dev/null
g fresh-et-onboard wait_online esci_et; log "fresh: esci_et onboarded (33 ONLINE)"
g fresh-et $PY freshness.py et --runs 1,2,3 >> results/logs/freshness.log 2>&1 || log "fresh: ET FAILED"
log "fresh: ET done"
stop_cluster; $PY freshness.py rollback >> results/logs/freshness.log 2>&1
for eng in es os; do
  log "fresh: $eng start"
  $PY search_baselines.py --engine $eng up > /dev/null 2>&1
  g fresh-$eng $PY freshness.py $eng --runs $([ $eng = es ] && echo 7,8,9 || echo 10,11,12) >> results/logs/freshness.log 2>&1 || log "fresh: $eng FAILED"
  $PY search_baselines.py --engine $eng down > /dev/null 2>&1
  $PY freshness.py rollback >> results/logs/freshness.log 2>&1
  log "fresh: $eng done (down)"
done
log "fresh: local start"
up_cluster
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_local.tableconfigs.json > /dev/null
mkdir -p $RUN/local
g fresh-local-ingest /opt/homebrew/opt/openjdk@25/bin/java -Xmx4g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED -Daws.endpointUrlS3=http://127.0.0.1:9020 \
  -cp "$RUN/conf:$STARTREE_PINOT_HOME/startree-tools/target/classes:$(cat stp-cache/startree-tools.full.cp)" \
  ai.startree.pinot.tools.admin.StarTreePinotAdministrator LaunchDataIngestionJob -jobSpecFile $E/pinot/esci_local.ingestion-job.pi.yaml > $RUN/local/ingest_fresh_base.log 2>&1
g fresh-local-online wait_online esci_local; log "fresh: esci_local base ingested (33 ONLINE)"
# local leftovers from chain3 (lost to the disk-full at 07:45), on this base-ingested esci_local before any append
log "local leftovers: hybrid_filter c8 latency + hybrid_sql_perseg (fresh local cluster, same base ingest)"
g local-lat-hf-c8 $PY pinot_runs.py --table local --latency --lat-modes hybrid_filter --lat-conc 8 \
  > results/logs/pinot_local_latency_hf_c8.log 2>&1 || log "local hybrid_filter c8 latency FAILED"
g local-perseg bash pinot/run_modes.sh local hybrid_sql_perseg
g fresh-local $PY freshness.py local --runs 4,5,6 >> results/logs/freshness.log 2>&1 || log "fresh: local FAILED"
log "fresh: local done"
stop_cluster; $PY freshness.py rollback >> results/logs/freshness.log 2>&1
log "fresh: done (table restored to original snapshot)"

# 3. storage, paused containers, final tables
$PY make_storage.py > results/logs/make_storage.log 2>&1 || log "make_storage FAILED"
docker start dc-zk startree-zookeeper startree-objectstore startree-iceberg-rest startree-controller startree-broker \
  startree-server startree-ui startree-notebook buildx_buildkit_ce-multi0 >> results/logs/restart_containers.log 2>&1 \
  || log "some paused containers failed to start (see results/logs/restart_containers.log)"
log "paused containers restarted"
$PY make_table1.py > results/logs/make_table1.log 2>&1 || log "make_table1 FAILED"
$PY make_timings.py > results/logs/make_timings.log 2>&1 || log "make_timings FAILED"
log "CHAIN5 ALL-DONE"
