#!/bin/bash
# Clean ET rerun: fresh cluster, onboard esci_et over esci.products_us_pi (timed), cold probe (first queries
# after onboarding), remaining quality modes, latency pass.
E=$EVAL_HOME
L=$E/results/sequential_run_log.md
RUN=$E/stp-cache/run
cd $E
sw() { sysctl -n vm.swapusage | awk '{print $6}' | tr -d M; }
log() { echo "- $(date '+%F %T') $* [swap $(sw)M]" >> $L; }
bash pinot/launch.sh > /dev/null
until [ "$(curl -s localhost:19000/instances | grep -o 'Server_' | wc -l)" -ge 3 ] && curl -s localhost:19000/instances | grep -q Minion; do sleep 2; done
log "et-rest: fresh cluster up (pid $(cat $RUN/quickstart.pid))"
mkdir -p $RUN/et
rm -f $RUN/et/watch.jsonl
nohup bash pinot/watch_table.sh esci_et_OFFLINE $RUN/et/watch.jsonl > /dev/null 2>&1 &
wpid=$!
T0=$(python3 -c 'import time;print(round(time.time(),2))')
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @pinot/esci_et.pi.tableconfigs.json >> $L; echo >> $L
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_et_OFFLINE" > /dev/null
log "et-rest: esci_et created + sync scheduled t0=$T0"
until python3 - "$RUN/et/watch.jsonl" <<'PY'
import json,sys
r=[json.loads(x) for x in open(sys.argv[1]) if x.strip()]
sys.exit(0 if r and r[-1]["is"]>=33 and r[-1]["ev_online"]>=33 else 1)
PY
do sleep 2; done
sleep 4; kill $wpid
log "et-rest: 33/33 ONLINE"
.venv/bin/python -I collect_timings_seq.py --table et --log $RUN/quickstart.log --watch $RUN/et/watch.jsonl --t0 $T0 \
  > results/logs/collect_timings_et.log 2>&1 || log "et-rest: collect_timings FAILED"
log "et-rest: timings -> results/pinot_et_timings.json (clean rerun)"
.venv/bin/python -I pinot_runs.py --table et --cold > results/logs/pinot_et_cold.log 2>&1 || log "et-rest: cold probe FAILED"
log "et-rest: cold probe done -> results/pinot_et_cold.json"
bash pinot/run_modes.sh et hybrid_filter hybrid_sql_perseg
s0=$(sw)
log "et latency pass start"
.venv/bin/python -I pinot_runs.py --table et --latency > results/logs/pinot_et_latency.log 2>&1 || log "et latency FAILED"
s1=$(sw)
log "et latency pass done (delta $(python3 -c "print(round(($s1-$s0)/1024,2))") GB$(python3 -c "print(' -> FLAG: latency measured under swap growth' if $s1-$s0>5120 else '')"))"
log "et-rest ALL-DONE"
