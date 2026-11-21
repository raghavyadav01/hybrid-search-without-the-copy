"""Scale the ESCI lake to ~1 TB for the cluster run: R replicas of the 1.2M products, ~512 MB files.

  python3 make_1tb.py BUCKET [REPLICAS] [WORKERS]          (on a large EC2 box with the instance role)

Replica 0 is the original catalog (same product ids, text and vectors), so quality can still be checked against
the ESCI judgments on it. Replica k > 0 gets product id `<id>-r<k>` and its embedding moved by Gaussian noise
(sigma 0.012 per dimension, then re-normalised: cosine ~0.95 to the original) so the vector index holds R distinct
neighbourhoods rather than exact copies; text, brand and the other columns are copied unchanged.
Writer settings match build_table_pi.py: zstd, 64 KB pages, 64-row write batches, page index, dictionary only on
brand/color/locale, one row group per file; Iceberg field ids are kept from the source files.
Writes to s3://BUCKET/warehouse/esci/products_1tb/data/ and prints a JSON summary.
"""
import json
import multiprocessing as mp
import os
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

SIGMA = 0.012
FILES_PER_REPLICA = 9  # 4.38 GB / 9 ~ 500 MB per file
PREFIX = os.environ.get("OUT_PREFIX", "warehouse/esci/products_1tb/data")
SORTED = os.environ.get("SORT_BY", "")  # e.g. brand: rows clustered by brand across all replicas
NFILES = 2052
WRITER = dict(compression="zstd", data_page_size=64 * 1024, write_batch_size=64, write_page_index=True,
              use_dictionary=["brand", "color", "locale"])
BASE = {}


def load(bucket):
    fs = pafs.S3FileSystem(region="us-west-2")
    sel = pafs.FileSelector(f"{bucket}/warehouse/esci/products_us_pi/data", recursive=True)
    files = sorted(f.path for f in fs.get_file_info(sel) if f.path.endswith(".parquet"))
    t = pa.concat_tables([pq.read_table(f, filesystem=fs, partitioning=None) for f in files])
    return t, files


def write_replica(args):
    bucket, k = args
    t = BASE["t"]
    if k > 0:
        rng = np.random.default_rng(1000 + k)
        emb_i = t.schema.get_field_index("embedding")
        flat = t.column(emb_i).combine_chunks()
        vals = flat.values.to_numpy(zero_copy_only=False).reshape(len(t), -1).astype(np.float32)
        vals = vals + rng.normal(0, SIGMA, vals.shape).astype(np.float32)
        vals /= np.linalg.norm(vals, axis=1, keepdims=True)
        field = t.schema.field(emb_i)
        emb = pa.FixedSizeListArray.from_arrays(pa.array(vals.ravel(), type=field.type.value_type), vals.shape[1]) \
            if pa.types.is_fixed_size_list(field.type) else \
            pa.ListArray.from_arrays(pa.array(np.arange(0, vals.size + 1, vals.shape[1], dtype=np.int32)),
                                     pa.array(vals.ravel(), type=field.type.value_type))
        t = t.set_column(emb_i, field, emb.cast(field.type))
        pid_i = t.schema.get_field_index("product_id")
        t = t.set_column(pid_i, t.schema.field(pid_i), pc.binary_join_element_wise(
            t.column(pid_i), pa.scalar(f"r{k}"), "-"))
    fs = pafs.S3FileSystem(region="us-west-2")
    rpf = -(-len(t) // FILES_PER_REPLICA)
    out = []
    for i in range(FILES_PER_REPLICA):
        part = t.slice(i * rpf, rpf)
        path = f"{bucket}/{PREFIX}/r{k:03d}-{i}.parquet"
        with fs.open_output_stream(path) as f:
            pq.write_table(part, f, row_group_size=len(part), **WRITER)
        out.append(path)
    return k, out


def noisy(t, ks):
    """Rows of t, row i taking replica ks[i]: replica 0 is the original row, k > 0 gets id `-r<k>` and a moved vector."""
    emb_i = t.schema.get_field_index("embedding"); field = t.schema.field(emb_i)
    vals = t.column(emb_i).combine_chunks().values.to_numpy(zero_copy_only=False).reshape(len(t), -1).astype(np.float32)
    rng = np.random.default_rng(int(ks[0]) * 7919 + len(t))
    mask = ks > 0
    vals[mask] += rng.normal(0, SIGMA, (int(mask.sum()), vals.shape[1])).astype(np.float32)
    vals[mask] /= np.linalg.norm(vals[mask], axis=1, keepdims=True)
    emb = pa.ListArray.from_arrays(pa.array(np.arange(0, vals.size + 1, vals.shape[1], dtype=np.int32)),
                                   pa.array(vals.ravel(), type=field.type.value_type))
    t = t.set_column(emb_i, field, emb.cast(field.type))
    pid_i = t.schema.get_field_index("product_id")
    suffix = pa.array(["" if k == 0 else f"-r{k}" for k in ks])
    return t.set_column(pid_i, t.schema.field(pid_i), pc.binary_join_element_wise(t.column(pid_i), suffix, ""))


def write_sorted_file(args):
    """File j of NFILES over the global order (base rows sorted by SORTED, each repeated for every replica)."""
    bucket, j, replicas = args
    t = BASE["t"]; total = len(t) * replicas
    g0, g1 = total * j // NFILES, total * (j + 1) // NFILES
    g = np.arange(g0, g1); base, ks = g // replicas, g % replicas
    part = noisy(t.take(pa.array(base)), ks)
    fs = pafs.S3FileSystem(region="us-west-2")
    path = f"{bucket}/{PREFIX}/s{j:04d}.parquet"
    with fs.open_output_stream(path) as f:
        pq.write_table(part, f, row_group_size=len(part), **WRITER)
    return j, [path]


def main():
    bucket = sys.argv[1]
    replicas = int(sys.argv[2]) if len(sys.argv) > 2 else 228
    workers = int(sys.argv[3]) if len(sys.argv) > 3 else 24
    t0 = time.time()
    BASE["t"], src = load(bucket)
    print(len(BASE["t"]), "rows from", len(src), "files in", round(time.time() - t0), "s", flush=True)
    if SORTED:
        BASE["t"] = BASE["t"].sort_by([(SORTED, "ascending"), ("product_id", "ascending")])
        jobs, fn = [(bucket, j, replicas) for j in range(NFILES)], write_sorted_file
    else:
        jobs, fn = [(bucket, k) for k in range(replicas)], write_replica
    with mp.get_context("fork").Pool(workers) as pool:
        for k, out in pool.imap_unordered(fn, jobs):
            if k % 50 == 0:
                print("done", k, round(time.time() - t0), "s", flush=True)
    fs = pafs.S3FileSystem(region="us-west-2")
    info = [f for f in fs.get_file_info(pafs.FileSelector(f"{bucket}/{PREFIX}")) if f.path.endswith(".parquet")]
    print(json.dumps({"files": len(info), "bytes": sum(f.size for f in info), "rows": len(BASE["t"]) * replicas,
                      "replicas": replicas, "sigma": SIGMA, "seconds": round(time.time() - t0)}))


if __name__ == "__main__":
    main()
