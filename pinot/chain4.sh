#!/bin/bash
# Freshness experiment, one system at a time: ET -> local -> ES -> OS. Then rollback + storage report.
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
RUN=$E/stp-cache/run
cd $E
sw() { sysctl -n vm.swapusage | awk '{print $6}'; }
log() { echo "- $(date '+%F %T') $* [swap $(sw)]" >> $L; }
PY=".venv/bin/python -I"
stop_cluster() { pid=$(cat $RUN/quickstart.pid); if ps -o command= -p $pid 2>/dev/null | grep -q S3ParquetQuickstart; then kill $pid; for i in $(seq 60); do kill -0 $pid 2>/dev/null || break; sleep 1; done; kill -0 $pid 2>/dev/null && kill -9 $pid; fi; rm -rf $RUN/tmp $RUN/quickstart.log; docker exec esci-minio mc rm --recursive --force local/pinot-tier/segments > /dev/null 2>&1; log "fresh: cluster stopped + cleaned (pid $pid)"; }
up_cluster() { bash pinot/launch.sh > /dev/null; until [ "$(curl -s localhost:19000/instances | grep -o 'Server_' | wc -l)" -ge 3 ] && curl -s localhost:19000/instances | grep -q Minion; do sleep 2; done; }
wait_online() { until [ "$(curl -s "localhost:19000/tables/$1/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; print(sum(1 for m in d.values() if "ONLINE" in m.values()))
except Exception: print(0)')" -ge 33 ]; do sleep 5; done; }

until grep -qE "^- [0-9-]+ [0-9:]+ CHAIN3 ALL-DONE$" $L; do sleep 30; done
$PY freshness.py rollback >> results/logs/freshness.log 2>&1
# ET
log "fresh: ET start"
up_cluster
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_et.pi.tableconfigs.json > /dev/null
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_et_OFFLINE" > /dev/null
wait_online esci_et; log "fresh: esci_et onboarded (33 ONLINE)"
$PY freshness.py et --runs 1,2,3 >> results/logs/freshness.log 2>&1 || log "fresh: ET FAILED"
log "fresh: ET done"
stop_cluster; $PY freshness.py rollback >> results/logs/freshness.log 2>&1
# local
log "fresh: local start"
up_cluster
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_local.tableconfigs.json > /dev/null
mkdir -p $RUN/local
/opt/homebrew/opt/openjdk@25/bin/java -Xmx4g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED -Daws.endpointUrlS3=http://127.0.0.1:9020 \
  -cp "$RUN/conf:$STARTREE_PINOT_HOME/startree-tools/target/classes:$(cat stp-cache/startree-tools.full.cp)" \
  ai.startree.pinot.tools.admin.StarTreePinotAdministrator LaunchDataIngestionJob -jobSpecFile $E/pinot/esci_local.ingestion-job.pi.yaml > $RUN/local/ingest_fresh_base.log 2>&1
wait_online esci_local; log "fresh: esci_local base ingested (33 ONLINE)"
$PY freshness.py local --runs 4,5,6 >> results/logs/freshness.log 2>&1 || log "fresh: local FAILED"
log "fresh: local done"
stop_cluster; $PY freshness.py rollback >> results/logs/freshness.log 2>&1
# ES, OS
for eng in es os; do
  log "fresh: $eng start"
  $PY search_baselines.py --engine $eng up > /dev/null 2>&1
  $PY freshness.py $eng --runs $([ $eng = es ] && echo 7,8,9 || echo 10,11,12) >> results/logs/freshness.log 2>&1 || log "fresh: $eng FAILED"
  $PY search_baselines.py --engine $eng down > /dev/null 2>&1
  log "fresh: $eng done (down)"
done
$PY freshness.py rollback >> results/logs/freshness.log 2>&1
$PY make_storage.py > /dev/null 2>&1
log "fresh: CHAIN4 ALL-DONE (table restored to original snapshot)"
