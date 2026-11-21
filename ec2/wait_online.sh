#!/bin/bash
# Poll esci_1tbb until all 2052 segments are ONLINE or one is in ERROR.
cd "$(dirname "$0")"
while true; do
  read n e < <(./g.sh PINOT 'curl -s "localhost:19000/tables/esci_1tbb/externalview?tableType=OFFLINE" | python3 -c "import json,sys; d=json.load(sys.stdin)[\"OFFLINE\"] or {}; v=[s for m in d.values() for s in m.values()]; print(v.count(\"ONLINE\"), v.count(\"ERROR\"))"' 2>/dev/null)
  echo "$(date +%T) online=${n:-?} error=${e:-?}"
  if [ "${n:-0}" -ge 2052 ] || [ "${e:-0}" -gt 0 ]; then break; fi
  sleep 300
done
./g.sh PINOT 'date +%T; df -h /data | tail -1'
