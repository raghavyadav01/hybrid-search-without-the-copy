"""ON the Pinot box: rewrite the same rows into ~512 MB files (Iceberg's default target size) as esci.products_us_big,
same writer settings as build_table_pi.py (zstd, 64 KB pages, page index, dictionary on brand/color/locale, 1 row group
per file), same row order.  python3 make_big.py BUCKET [TARGET_MB]"""
import json, sys, time
from pathlib import Path
sys.path.insert(0, str(Path.home() / "eval"))
import pyarrow as pa, pyarrow.fs as pafs, pyarrow.parquet as pq
import build_table_pi  # noqa: F401  (patches the writer: page index, per-column dictionary)
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import IdentityTransform

bucket = sys.argv[1]; target_mb = float(sys.argv[2]) if len(sys.argv) > 2 else 512
sort_by = sys.argv[3] if len(sys.argv) > 3 else ""
name = sys.argv[4] if len(sys.argv) > 4 else "esci.products_us_big"
cat = load_catalog("rest", type="rest", uri="http://localhost:8190", **{"s3.region": "us-west-2"})
src = cat.load_table("esci.products_us_pi")
fs = pafs.S3FileSystem(region="us-west-2")
files = sorted(src.inspect.files().to_pandas().file_path)
t = pa.concat_tables([pq.read_table(f.replace("s3://", ""), filesystem=fs, partitioning=None) for f in files])
t = t.cast(src.schema().as_arrow()) if t.schema != src.schema().as_arrow() else t
total_mb = sum(fs.get_file_info(f.replace("s3://", "")).size for f in files) / 2**20
nfiles = max(1, round(total_mb / target_mb)); rpf = -(-t.num_rows // nfiles)
if sort_by:
    t = t.sort_by([(sort_by, "ascending"), ("product_id", "ascending")])
print(t.num_rows, "rows", round(total_mb), "MB ->", nfiles, "files of", rpf, "rows", flush=True)
if cat.table_exists(name):
    cat.drop_table(name)
props = {"format-version": "2", "write.parquet.compression-codec": "zstd",
         "write.target-file-size-bytes": str(4 * 1024 ** 3), "write.parquet.row-group-limit": str(10_000_000),
         "write.parquet.page-size-bytes": str(64 * 1024), "write.parquet.page-row-limit": "64"}
tbl = cat.create_table(name, schema=src.schema(), properties=props,
                       location=f"s3://{bucket}/warehouse/" + name.replace(".", "/"))
with tbl.update_spec() as us:
    us.add_field("locale", IdentityTransform(), "locale")
tbl = cat.load_table(name)
t0 = time.time()
with tbl.transaction() as tx:
    for s in range(0, t.num_rows, rpf):
        tx.append(t.slice(s, rpf))
        print("appended", s, round(time.time() - t0), "s", flush=True)
tbl = cat.load_table(name)
fl = tbl.inspect.files().to_pandas()
print(json.dumps({"files": len(fl), "rows": int(fl.record_count.sum()), "mb": round(fl.file_size_in_bytes.sum() / 2**20),
                  "write_s": round(time.time() - t0, 1)}))
