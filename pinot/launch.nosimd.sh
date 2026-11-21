#!/bin/bash
# Launch the port-shifted S3ParquetQuickstart for the ESCI eval (see papers/eval/stp-build.md).
export JAVA_HOME=/opt/homebrew/opt/openjdk@25
STP=$STARTREE_PINOT_HOME
E=$EVAL_HOME
CACHE=$E/stp-cache/run
mkdir -p $CACHE/tmp
nohup "$JAVA_HOME/bin/java" -Xms1g -Xmx8g -XX:MaxDirectMemorySize=8g \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  -Dio.netty.tryReflectionSetAccessible=true -Dlog4j2.configurationFile=$CACHE/conf/log4j2-eval.xml \
  -Djava.io.tmpdir=$CACHE/tmp \
  -Daws.endpointUrlS3=http://127.0.0.1:9020 \
  -cp "$CACHE/conf:$E/stp-cache/port-shift/classes:$STP/startree-tools/target/classes:$(cat $E/stp-cache/startree-tools.full.cp)" \
  ai.startree.pinot.tools.quickstart.S3ParquetQuickstart \
  -region us-east-1 -bucket pinot-tier \
  -accessKey minioadmin -secretKey minioadmin -sessionToken "" \
  -configFile $EVAL_HOME/stp-cache/run/overrides.properties -enablePreloadCache -preloadTotalSize 40G -preloadIndexKeys '*.vector_index,*.text_index' \
  > "$CACHE/quickstart.log" 2>&1 &
echo $! > "$CACHE/quickstart.pid"
echo "pid $(cat $CACHE/quickstart.pid)"
