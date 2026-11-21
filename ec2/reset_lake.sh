#!/bin/bash
# ON the Pinot box: restore the lake table to the original 33 files (pyiceberg 0.9 has no set_current_snapshot)
BUCKET=$1
P=s3://$BUCKET/warehouse/esci/products_us_pi/data/locale=us/
aws s3 ls $P | awk -v p=$P '{print p$4}' | grep parquet | sort > /tmp/now.txt
sort ~/lake_files.txt > /tmp/orig.txt
comm -23 /tmp/now.txt /tmp/orig.txt | while read f; do aws s3 rm --quiet "$f"; done
AWS_DEFAULT_REGION=us-west-2 python3 ~/make_lake.py register http://localhost:8190 $BUCKET 2>&1 | tail -1
