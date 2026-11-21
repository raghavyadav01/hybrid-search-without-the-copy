#!/bin/bash
# Pack the exact Pinot build used locally (same jars, same classes) for the EC2 Pinot box.
set -euo pipefail
E=$EVAL_HOME
STP=$STARTREE_PINOT_HOME
OUT=${1:-$E/ec2/build}
rm -rf $OUT; mkdir -p $OUT/lib $OUT/classes
tr ':' '\n' < $E/stp-cache/startree-tools.full.cp | while read -r j; do [ -f "$j" ] && cp "$j" $OUT/lib/; done
cp -R $E/stp-cache/port-shift/classes $OUT/classes/port-shift
cp -R $STP/startree-tools/target/classes $OUT/classes/startree-tools
cp -R $E/stp-cache/run/conf $OUT/conf
cp -R $E/stp-cache/lucene-simd $OUT/classes/lucene-simd
cp $E/pinot/esci_et.fast.tableconfigs.json $E/stp-cache/run/overrides.properties $OUT/
# record exact source revisions
{ echo "oss: $(git -C $PINOT_HOME rev-parse HEAD) + uncommitted:"; \
  git -C $PINOT_HOME status --short; \
  echo "stp: $(git -C $STP rev-parse HEAD) + uncommitted:"; git -C $STP status --short; } > $OUT/REVISIONS.txt
tar -C $(dirname $OUT) -czf $OUT.tgz $(basename $OUT)
ls -lh $OUT.tgz; ls $OUT/lib | wc -l
