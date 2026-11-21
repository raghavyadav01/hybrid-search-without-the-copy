BUCKET=$1; REGION=$2
pkill -f '[S]3ParquetQuickstart'; while pgrep -f '[S]3ParquetQuickstart' >/dev/null; do sleep 2; done; sleep 3
bash ~/run_pinot.sh $BUCKET $REGION 24g
until curl -s localhost:19000/instances 2>/dev/null | grep -q Minion; do sleep 3; done
T0=$(date +%s)
for t in esci_et esci_big esci_brand esci_brandinv; do
  curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @$HOME/$t.ec2.json | head -c 120; echo
  curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=${t}_OFFLINE" > /dev/null &
done
for t in esci_et:33 esci_big:8 esci_brand:33 esci_brandinv:33; do
  n=${t#*:}; t=${t%:*}
  until [ "$(curl -s "localhost:19000/tables/$t/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; print(sum(1 for m in d.values() if "ONLINE" in m.values()))
except Exception: print(0)')" -ge $n ]; do
    curl -s "localhost:19000/tables/$t/externalview?tableType=OFFLINE" | grep -q '"ERROR"' && { echo "$t SEG-ERROR"; break; }
    sleep 5; done
  echo "$t online after $(( $(date +%s) - T0 ))s"
done
