until curl -s localhost:19000/instances | grep -q Minion; do sleep 3; done
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @esci_brand.ec2.json | head -c 200; echo
T0=$(date +%s.%N)
curl -s -X POST "localhost:19000/tasks/schedule?taskType=ExternalTableSyncTask&tableName=esci_brand_OFFLINE" > /dev/null &
FIRST=""
while true; do
  n=$(curl -s "localhost:19000/tables/esci_brand/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; print(sum(1 for m in d.values() if "ONLINE" in m.values()))
except Exception: print(0)')
  [ -z "$FIRST" ] && [ "$n" -ge 1 ] && FIRST=$(python3 -c "print(round($(date +%s.%N)-$T0,1))")
  [ "$n" -ge 33 ] && break
  if curl -s "localhost:19000/tables/esci_brand/externalview?tableType=OFFLINE" | grep -q '"ERROR"'; then echo SEG-ERROR; break; fi
  sleep 2
done
echo "onboard first=${FIRST}s all=$(python3 -c "print(round($(date +%s.%N)-$T0,1))")s n=$n"
curl -s -X POST localhost:18000/query/sql -H 'Content-Type: application/json' -d '{"sql":"select count(*) from esci_brand"}' | grep -o '"rows":\[\[[0-9]*\]\]'
