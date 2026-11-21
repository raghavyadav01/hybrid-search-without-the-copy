"""ON the Pinot box: register the generated 1 TB files as esci.products_1tb (same schema, spec and properties as
esci.products_us_pi) in the local Iceberg REST catalog.  python3 register_1tb.py BUCKET"""
import json
import sys
import time
from pathlib import Path

import boto3
import pyiceberg.io.pyarrow as pyi
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema

bucket = sys.argv[1]
name = sys.argv[2] if len(sys.argv) > 2 else "products_1tb"
here = Path.home() / "lake"
schema = Schema.model_validate_json((here / "schema.json").read_text())
spec = PartitionSpec.model_validate_json((here / "spec.json").read_text())
cat = load_catalog("rest", type="rest", uri="http://localhost:8190", warehouse=f"s3://{bucket}/warehouse/")
try:
    cat.create_namespace("esci")
except Exception:
    pass
# The generator keeps the Iceberg field ids of the source files; add_files rejects files that carry ids
_visit = pyi.visit_pyarrow
pyi.visit_pyarrow = lambda s, v: False if isinstance(v, pyi._HasIds) else _visit(s, v)
if cat.table_exists(f"esci.{name}"):
    cat.drop_table(f"esci.{name}")
t = cat.create_table(f"esci.{name}", schema=schema, partition_spec=spec,
                     location=f"s3://{bucket}/warehouse/esci/{name}",
                     properties=json.loads((here / "properties.json").read_text()))
keys = []
for page in boto3.client("s3").get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"warehouse/esci/{name}/data/"):
    keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
t0 = time.time()
t.add_files([f"s3://{bucket}/{k}" for k in sorted(keys)])
print("registered", len(keys), "files in", round(time.time() - t0), "s; snapshot", t.current_snapshot().snapshot_id)
