#!/bin/bash
# After provision.sh: upload lake + build, set up all boxes. Run from papers/eval/ec2.
set -euo pipefail
cd "$(dirname "$0")"; set -a; eval "$(cat hosts.env)"; set +a
SSH="ssh -i $KEY -o StrictHostKeyChecking=no -o ServerAliveInterval=30"; SCP="scp -i $KEY -o StrictHostKeyChecking=no"
export AWS_REGION=$REGION AWS_PAGER=""
# 1. lake files -> S3 (byte-identical to the laptop run)
aws s3 sync --only-show-errors lake/ s3://$BUCKET/warehouse/esci/products_us_pi/data/locale=us/ --exclude '*' --include '*.parquet'
aws s3 ls s3://$BUCKET/warehouse/esci/products_us_pi/data/locale=us/ | awk '{print "s3://'$BUCKET'/warehouse/esci/products_us_pi/data/locale=us/"$4}' > lake_files.txt
wc -l lake_files.txt
python3 ec2_tableconfig.py ../pinot/esci_et.fast.tableconfigs.json esci_et.ec2.json $BUCKET $REGION
# 2. all boxes: base packages + client bundle
for h in $PINOT_PUB $ES_PUB $OS_PUB $CLIENT_PUB; do
  ( $SCP client.tgz lake_files.txt ec2-user@$h: && $SSH ec2-user@$h \
    'sudo dnf install -y -q docker python3-pip tar gzip >/dev/null && sudo systemctl enable --now docker && sudo usermod -aG docker ec2-user && pip3 install -q --user numpy pyarrow pandas requests "pyiceberg[pyarrow]==0.9.1" boto3 && mkdir -p eval && tar -xzf client.tgz -C eval' ) &
done; wait
# 3. Pinot box: build + lake registration + cluster
$SCP build.tgz box_pinot.sh run_pinot.sh make_lake.py esci_et.ec2.json ec2-user@$PINOT_PUB:
$SSH ec2-user@$PINOT_PUB "mkdir -p lake" && $SCP lake/schema.json lake/spec.json lake/properties.json ec2-user@$PINOT_PUB:lake/
$SSH ec2-user@$PINOT_PUB "bash box_pinot.sh $BUCKET $REGION"
echo bootstrap-done
