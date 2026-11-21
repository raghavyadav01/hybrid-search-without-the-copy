#!/bin/bash
# Runs ON the Pinot EC2 box: same JVM flags as the laptop run (pinot/launch.sh) except heap sized to the box and
# real S3 instead of MinIO. Args: BUCKET REGION HEAP
set -euo pipefail
BUCKET=$1 REGION=$2 HEAP=${3:-20g}
B=~/build; sudo mkdir -p /data/run/tmp && sudo chown -R ec2-user /data; ln -sfn /data/run ~/run
# instance-role credentials (the quickstart wants explicit keys)
TOK=$(curl -s -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
ROLE=$(curl -s -H "X-aws-ec2-metadata-token: $TOK" http://169.254.169.254/latest/meta-data/iam/security-credentials/)
CREDS=$(curl -s -H "X-aws-ec2-metadata-token: $TOK" http://169.254.169.254/latest/meta-data/iam/security-credentials/$ROLE)
AK=$(echo "$CREDS" | python3 -c "import json,sys;print(json.load(sys.stdin)['AccessKeyId'])")
SK=$(echo "$CREDS" | python3 -c "import json,sys;print(json.load(sys.stdin)['SecretAccessKey'])")
ST=$(echo "$CREDS" | python3 -c "import json,sys;print(json.load(sys.stdin)['Token'])")
JAVA=$(ls -d /usr/lib/jvm/java-25-amazon-corretto*/bin/java | head -1)
nohup $JAVA -Xms4g -Xmx$HEAP -XX:MaxDirectMemorySize=$HEAP --add-modules jdk.incubator.vector \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -Dio.netty.tryReflectionSetAccessible=true -Dlog4j2.configurationFile=$B/conf/log4j2-eval.xml \
  -Djava.io.tmpdir=/data/run/tmp \
  -cp "$B/classes/lucene-simd:$B/conf:$B/classes/port-shift:$B/classes/startree-tools:$B/lib/*" \
  ai.startree.pinot.tools.quickstart.S3ParquetQuickstart -region $REGION -bucket $BUCKET -accessKey "$AK" -secretKey "$SK" -sessionToken "$ST" \
  -configFile ~/build/overrides.properties -enablePreloadCache -preloadTotalSize ${PRELOAD:-30G} \
  -preloadIndexKeys '*.vector_index,*.text_index' > ~/run/quickstart.log 2>&1 &
echo $! > ~/run/quickstart.pid
