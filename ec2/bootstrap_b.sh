#!/bin/bash
# After provision_b.sh: lake -> S3, table configs, set up both Pinot boxes and the client. Run from papers/eval/ec2.
set -euo pipefail
cd "$(dirname "$0")"; set -a; eval "$(cat hostsb.env)"; set +a
export AWS_PROFILE=${AWS_PROFILE:-isir-bench} AWS_REGION=$REGION AWS_PAGER=""
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ServerAliveInterval=30"; SCP="scp -i $KEY -o StrictHostKeyChecking=no"
aws s3 sync --only-show-errors lake/ s3://$BUCKET/warehouse/esci/products_us_pi/data/locale=us/ --exclude '*' --include '*.parquet'
echo "lake files in S3: $(aws s3 ls s3://$BUCKET/warehouse/esci/products_us_pi/data/locale=us/ | grep -c parquet)"
for n in 1 2; do
  python3 ec2_tableconfig.py ../pinot/esci_et.fast.tableconfigs.json esci_et.p$n.json $BUCKET $REGION
  python3 - esci_et.p$n.json $n $P1_PRIV <<'PY'
import json, sys
f, n, cat = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.load(open(f)); o = d["offline"]
o["task"]["taskTypeConfigsMap"]["ExternalTableSyncTask"]["catalog.iceberg-rest.restUri"] = f"http://{cat}:8190"
o["tierConfigs"][0]["tierBackendProperties"]["pathPrefix"] = f"segments-p{n}/esci_et"
json.dump(d, open(f, "w"), indent=2)
PY
done
( $SCP client.tgz ../mpbench.py ec2-user@$CLIENT_PUB: && $SSH ec2-user@$CLIENT_PUB \
  'sudo dnf install -y -q python3-pip tar gzip >/dev/null && pip3 install -q --user numpy pyarrow pandas requests && mkdir -p eval && tar -xzf client.tgz -C eval && cp mpbench.py eval/' && echo client-ready ) &
for n in 1 2; do
  H=$(eval echo \$P${n}_PUB); ROLE=$([ $n = 1 ] && echo catalog || echo replica)
  ( $SCP build.tgz core-site.ec2.xml box_pinot_b.sh run_pinot_b.sh make_lake.py esci_et.p$n.json ec2-user@$H: \
    && $SSH ec2-user@$H "mkdir -p lake" && $SCP lake/schema.json lake/spec.json lake/properties.json ec2-user@$H:lake/ \
    && $SSH ec2-user@$H "bash box_pinot_b.sh $BUCKET $REGION $ROLE" > ../logs/box_p$n.log 2>&1 && echo "p$n ready" ) &
done
wait
echo bootstrap-done
