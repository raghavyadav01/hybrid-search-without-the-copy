"""Step 3: write the US product table (text + bge-base embedding) as an Iceberg table on MinIO.

  docker compose -p esci-lake -f docker-compose.yml up -d
  .venv/bin/python -I build_table.py [--target-mb 128] [--table esci.products_us] [--limit N]

Catalog: Iceberg REST (apache/iceberg-rest-fixture, SQLite JDBC backend) at
http://localhost:8190, warehouse s3://esci-warehouse/ on MinIO http://localhost:9020.
Files are written by pyiceberg (so they carry Iceberg field-ids), zstd, one row group per file,
partitioned by identity(locale). Each append in the single transaction becomes one data file;
rows per file are sized from a calibration write so files land near --target-mb on disk.
The local products_us.parquet + embedding shards stay as the input for the reference baselines.
"""
import argparse
import io
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from common import DIM, EMB_DIR, PRODUCTS_US, RESULTS_DIR

REST_URI = os.environ.get("ESCI_REST_URI", "http://localhost:8190")
S3_ENDPOINT = "http://localhost:9020"


def catalog():
    if os.environ.get("ESCI_EC2"):  # EC2: REST catalog on the box, real S3 via the instance role
        return load_catalog("esci", **{"type": "rest", "uri": REST_URI, "s3.region": os.environ.get("AWS_REGION",
                                                                                                    "us-west-2")})
    return load_catalog("esci", **{
        "type": "rest", "uri": REST_URI,
        "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin", "s3.region": "us-east-1",
        "s3.path-style-access": "true"})


def load_joined(limit=None):
    prod = pq.read_table(PRODUCTS_US)
    shards = sorted(EMB_DIR.glob("shard-*.parquet"))
    emb = pa.concat_tables([pq.read_table(s) for s in shards])
    if limit:
        prod, emb = prod.slice(0, limit), emb.slice(0, limit)
    assert emb.num_rows == prod.num_rows, (emb.num_rows, prod.num_rows)
    assert pc.all(pc.equal(emb.column("product_id"), prod.column("product_id"))).as_py(), \
        "embedding shards out of order with products_us.parquet"
    vec = emb.column("embedding").combine_chunks()
    vals = vec.values if isinstance(vec, pa.FixedSizeListArray) else vec.flatten()
    assert len(vals) == DIM * prod.num_rows
    elem = pa.field("element", pa.float32(), nullable=False)
    offsets = pa.array(range(0, (prod.num_rows + 1) * DIM, DIM), pa.int32())
    lst = pa.ListArray.from_arrays(offsets, vals, type=pa.list_(elem))
    schema = pa.schema([
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("title", pa.string()), pa.field("description", pa.string()),
        pa.field("bullet_point", pa.string()), pa.field("brand", pa.string()),
        pa.field("color", pa.string()), pa.field("locale", pa.string(), nullable=False),
        pa.field("text", pa.string(), nullable=False),
        pa.field("embedding", pa.list_(elem), nullable=False)])
    cols = [prod.column(n).combine_chunks() for n in schema.names[:-1]] + [lst]
    return pa.Table.from_arrays(cols, schema=schema)


def rows_per_file(t, target_mb):
    sample = t.slice(0, 20_000)
    buf = io.BytesIO()
    pq.write_table(sample, buf, compression="zstd")
    per_row = buf.tell() / sample.num_rows
    return max(1000, int(target_mb * 1024 * 1024 / per_row)), per_row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="esci.products_us")
    ap.add_argument("--target-mb", type=float, default=128)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--replace", action="store_true")
    a = ap.parse_args()

    t = load_joined(a.limit)
    rpf, per_row = rows_per_file(t, a.target_mb)
    print(f"{t.num_rows} rows, ~{per_row:.0f} B/row on disk -> {rpf} rows/file", flush=True)

    cat = catalog()
    ns = a.table.split(".")[0]
    if (ns,) not in cat.list_namespaces():
        cat.create_namespace(ns)
    if cat.table_exists(a.table):
        if not a.replace:
            sys.exit(f"{a.table} exists; pass --replace")
        cat.purge_table(a.table)
    tbl = cat.create_table(a.table, schema=t.schema, properties={
        "format-version": "2",
        "write.parquet.compression-codec": "zstd",
        # Keep pyiceberg from splitting an append; we size each append to one target file.
        "write.target-file-size-bytes": str(4 * 1024 ** 3),
        "write.parquet.row-group-limit": str(10_000_000),
    })
    loc_id = tbl.schema().find_field("locale").field_id
    with tbl.update_spec() as us:
        us.add_field("locale", IdentityTransform(), "locale")
    tbl = cat.load_table(a.table)

    t0 = time.time()
    with tbl.transaction() as tx:
        for s in range(0, t.num_rows, rpf):
            tx.append(t.slice(s, rpf))
            print(f"  appended rows {s}..{min(s + rpf, t.num_rows)}", flush=True)
    secs = time.time() - t0

    tbl = cat.load_table(a.table)
    files = tbl.inspect.files().to_pandas()
    info = {
        "table": a.table, "catalog": {"type": "rest", "uri": REST_URI,
                                      "uri_in_docker_network": "http://iceberg-rest:8181",
                                      "backend": "iceberg-rest-fixture 1.9.1, SQLite JDBC"},
        "warehouse": "s3://esci-warehouse/", "s3_endpoint_host": S3_ENDPOINT,
        "s3_endpoint_in_docker_network": "http://minio:9000",
        "metadata_location": tbl.metadata_location, "location": tbl.location(),
        "format_version": tbl.format_version, "partition_spec": str(tbl.spec()),
        "rows": int(files.record_count.sum()), "data_files": len(files),
        "file_mb_min": round(files.file_size_in_bytes.min() / 2 ** 20, 1),
        "file_mb_median": round(files.file_size_in_bytes.median() / 2 ** 20, 1),
        "file_mb_max": round(files.file_size_in_bytes.max() / 2 ** 20, 1),
        "total_gb": round(files.file_size_in_bytes.sum() / 2 ** 30, 2),
        "snapshots": len(tbl.metadata.snapshots), "current_snapshot_id":
            tbl.current_snapshot().snapshot_id, "write_secs": round(secs, 1),
        "schema": str(tbl.schema()), "locale_field_id": loc_id,
    }
    if not a.limit:
        (RESULTS_DIR / f"iceberg_{a.table.replace('.', '_')}.json").write_text(
            json.dumps(info, indent=2))
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
