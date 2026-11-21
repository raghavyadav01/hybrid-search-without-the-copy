B=~/build; JAVA=/usr/lib/jvm/java-25-amazon-corretto/bin/java; mkdir -p ~/local
curl -s -X POST -H 'Content-Type: application/json' localhost:19000/tableConfigs -d @$HOME/esci_local.ec2.json | head -c 150; echo
T0=$(date +%s.%N)
$JAVA -Xmx16g --add-modules jdk.incubator.vector --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  -cp "$B/classes/lucene-simd:$B/conf:$B/classes/port-shift:$B/classes/startree-tools:$B/lib/*" \
  ai.startree.pinot.tools.admin.StarTreePinotAdministrator LaunchDataIngestionJob -jobSpecFile $HOME/esci_local.ingest.ec2.yaml > ~/local/ingest.log 2>&1
echo "job rc=$? wall=$(python3 -c "print(round($(date +%s.%N)-$T0,1))")s"
until [ "$(curl -s "localhost:19000/tables/esci_local/externalview?tableType=OFFLINE" | python3 -c 'import json,sys
try:
  d=json.load(sys.stdin)["OFFLINE"] or {}; print(sum(1 for m in d.values() if "ONLINE" in m.values()))
except Exception: print(0)')" -ge 33 ]; do sleep 3; done
echo "local queryable after $(python3 -c "print(round($(date +%s.%N)-$T0,1))")s"
curl -s -X POST localhost:18000/query/sql -H 'Content-Type: application/json' -d '{"sql":"select count(*) from esci_local"}' | grep -o '"rows":\[\[[0-9]*\]\]'
