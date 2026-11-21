#!/bin/bash
# Runs ON the i4i.4xlarge Pinot box (AL2023 x86_64): local NVMe at /data, Java 25, Iceberg REST catalog.
set -euxo pipefail
BUCKET=$1 REGION=$2
DEV=$(lsblk -dpno NAME,MODEL | grep -i "Instance Storage" | awk '{print $1}' | head -1)
sudo mkfs.xfs -f $DEV && sudo mkdir -p /data && sudo mount -o noatime $DEV /data && sudo chown ec2-user /data
sudo dnf install -y -q java-25-amazon-corretto-headless docker python3-pip tar gzip
sudo systemctl enable --now docker
timeout 600 pip3 install -q --user "pyiceberg[s3fs,pyarrow]==0.9.1" boto3 requests
tar -xzf ~/build.tgz -C ~
sudo docker run -d --name iceberg-rest --restart unless-stopped -p 8190:8181 \
  -e CATALOG_WAREHOUSE=s3://$BUCKET/warehouse/ -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e AWS_REGION=$REGION -e CATALOG_URI=jdbc:sqlite:/tmp/cat.db apache/iceberg-rest-fixture:1.9.1
until curl -sf localhost:8190/v1/config >/dev/null; do sleep 2; done
df -h /data; nproc; free -g | head -2
echo box-ready
