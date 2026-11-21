#!/bin/bash
# Runs ON the Pinot EC2 box (AL2023 arm64). Args: BUCKET REGION. Expects ~/build.tgz and ~/lake/ (schema/spec/properties json).
set -euxo pipefail
BUCKET=$1 REGION=$2
sudo dnf install -y -q java-25-amazon-corretto-headless docker python3-pip tar gzip
sudo systemctl enable --now docker
pip3 install -q --user "pyiceberg[s3fs,pyarrow]==0.9.1" boto3 requests
tar -xzf ~/build.tgz -C ~
# Iceberg REST catalog backed by S3 (instance-role credentials via the default chain)
sudo docker run -d --name iceberg-rest --restart unless-stopped -p 8190:8181 \
  -e CATALOG_WAREHOUSE=s3://$BUCKET/warehouse/ -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e AWS_REGION=$REGION -e CATALOG_URI=jdbc:sqlite:/tmp/cat.db apache/iceberg-rest-fixture:1.9.1
until curl -sf localhost:8190/v1/config >/dev/null; do sleep 2; done
python3 ~/make_lake.py register http://localhost:8190 $BUCKET
