#!/bin/bash
# ON the Pinot box: create esci_1tb, trigger the sync, log ONLINE segment count every 60 s until all 2052 are online.
cd ~
until curl -s localhost:19000/instances | grep -q Minion; do sleep 3; done
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @esci_1tb.ec2.json | head -c 200; echo
T0=$(date +%s)
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_1tb_OFFLINE" | head -c 200; echo
while true; do
  read n e < <(curl -s "localhost:19000/tables/esci_1tb/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; v=[s for m in d.values() for s in m.values()]
  print(v.count("ONLINE"), v.count("ERROR"))
except Exception: print(0, 0)')
  echo "$(date +%T) t=$(( $(date +%s) - T0 ))s online=$n error=$e disk=$(df -h /data | awk 'NR==2{print $3}')"
  [ "$n" -ge 2052 ] && break
  sleep 60
done
echo "ALL ONLINE in $(( $(date +%s) - T0 ))s"
curl -s -X POST localhost:18000/query/sql -H 'Content-Type: application/json' -d '{"sql":"select count(*) from esci_1tb"}' | grep -o '"rows":\[\[[0-9]*\]\]'
