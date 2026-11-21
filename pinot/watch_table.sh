#!/bin/bash
# Polls the port-shifted controller every 2 s and appends one JSON line per poll:
#   {"t": epoch, "is": #segments in IdealState, "ev_online": #segments ONLINE in ExternalView}
# Usage: watch_table.sh <tableNameWithType> <out.jsonl>   (stop with kill)
T=$1; OUT=$2
while true; do
  now=$(python3 -c 'import time;print(round(time.time(),2))')
  is=$(curl -s -m5 "localhost:19000/tables/${T%_OFFLINE}/idealstate?tableType=OFFLINE" | python3 -c 'import json,sys
try: print(len(json.load(sys.stdin)["OFFLINE"]))
except Exception: print(-1)')
  ev=$(curl -s -m5 "localhost:19000/tables/${T%_OFFLINE}/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}
  print(sum(1 for s,m in d.items() if any(v=="ONLINE" for v in m.values())))
except Exception: print(-1)')
  echo "{\"t\": $now, \"is\": $is, \"ev_online\": $ev}" >> "$OUT"
  sleep 2
done
