#!/bin/bash
# ON a camera-ready Pinot box (AL2023 arm64). Args: BUCKET REGION ROLE(catalog|replica)
set -euxo pipefail
BUCKET=$1 REGION=$2 ROLE=$3
sudo dnf install -y -q java-25-amazon-corretto-headless docker python3-pip tar gzip
sudo systemctl enable --now docker
tar -xzf ~/build.tgz -C ~ && cp ~/core-site.ec2.xml ~/build/conf/core-site.xml   # S3A -> real S3, not laptop MinIO
if [ "$ROLE" = catalog ]; then
  # no s3fs extra: its aiobotocore pins send pip into endless backtracking against the system awscli
  pip3 install -q --user "pyiceberg[pyarrow]==0.9.1" boto3 requests
  sudo docker run -d --name iceberg-rest --restart unless-stopped -p 8190:8181 \
    -e CATALOG_WAREHOUSE=s3://$BUCKET/warehouse/ -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
    -e AWS_REGION=$REGION -e CATALOG_URI=jdbc:sqlite:/tmp/cat.db apache/iceberg-rest-fixture:1.9.1
  until curl -sf localhost:8190/v1/config >/dev/null; do sleep 2; done
  python3 ~/make_lake.py register http://localhost:8190 $BUCKET
fi
echo box-ready
