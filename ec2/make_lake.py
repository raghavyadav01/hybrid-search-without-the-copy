"""Copy the current snapshot's data files of esci.products_us_pi from local MinIO to S3 and register them as a new
Iceberg table (same schema + partition spec) in the EC2 REST catalog. Files are byte-identical to the local run.

  .venv/bin/python -I ec2/make_lake.py export  DIR          (MinIO -> DIR, lists files of the current snapshot)
  .venv/bin/python -I ec2/make_lake.py register URI BUCKET   (on the Pinot box: add_files from s3://BUCKET/...)
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def export(out):
    import pyarrow.fs as pafs
    from build_table import catalog
    t = catalog().load_table("esci.products_us_pi")
    files = [task.file.file_path for task in t.scan().plan_files()]
    fs = pafs.S3FileSystem(endpoint_override="http://127.0.0.1:9020", access_key="minioadmin",
                           secret_key="minioadmin", scheme="http", region="us-east-1")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    for f in files:
        name = f.rsplit("/", 1)[1]
        with fs.open_input_stream(f.replace("s3://", "")) as src, open(out / name, "wb") as dst:
            dst.write(src.read())
    (out / "schema.json").write_text(t.schema().model_dump_json())
    (out / "spec.json").write_text(t.spec().model_dump_json())
    (out / "properties.json").write_text(json.dumps(t.properties))
    print(len(files), "files ->", out)


def register(uri, bucket):
    from pyiceberg.catalog import load_catalog
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.schema import Schema
    import boto3
    here = Path(__file__).resolve().parent / "lake"
    schema = Schema.model_validate_json((here / "schema.json").read_text())
    spec = PartitionSpec.model_validate_json((here / "spec.json").read_text())
    cat = load_catalog("rest", type="rest", uri=uri, warehouse=f"s3://{bucket}/warehouse/")
    try:
        cat.create_namespace("esci")
    except Exception:
        pass
    import pyiceberg.io.pyarrow as pyi
    # The files were written by Iceberg for this exact schema, so their field ids match it; add_files rejects any ids
    _visit = pyi.visit_pyarrow
    pyi.visit_pyarrow = lambda s, v: False if isinstance(v, pyi._HasIds) else _visit(s, v)
    if cat.table_exists("esci.products_us_pi"):
        cat.drop_table("esci.products_us_pi")
    t = cat.create_table("esci.products_us_pi", schema=schema, partition_spec=spec,
                         location=f"s3://{bucket}/warehouse/esci/products_us_pi",
                         properties=json.loads((here / "properties.json").read_text()))
    s3 = boto3.client("s3")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket, Prefix="warehouse/esci/products_us_pi/data/")
            .get("Contents", []) if o["Key"].endswith(".parquet")]
    t.add_files([f"s3://{bucket}/{k}" for k in keys])
    print("registered", len(keys), "files; snapshot", t.current_snapshot().snapshot_id)


if __name__ == "__main__":
    {"export": lambda: export(sys.argv[2]), "register": lambda: register(sys.argv[2], sys.argv[3])}[sys.argv[1]]()
