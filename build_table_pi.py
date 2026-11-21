"""Rewrite of the lake table with a Parquet page index and small pages: esci.products_us_pi.

  .venv/bin/python -I build_table_pi.py [--table esci.products_us_pi] [--target-mb 128] [--page-kb 64]
  .venv/bin/python -I build_table_pi.py --stats-only        (re-inspect the written files)

Same rows, columns, order, partitioning (identity(locale)) and ~128 MB files as build_table.py
(esci.products_us, which is kept). Differences, all in the Parquet writer:
  - page index written (ColumnIndex + OffsetIndex per column chunk: write_page_index=True);
  - data page size 64 KB for every column (data_page_size=65536) and a write batch of 64 levels
    (Iceberg write.parquet.page-row-limit=64 -> pyarrow write_batch_size), so a page closes near
    64 KB: ~21 vectors (768 x float32 = 3 KB each) or ~60 product texts per page;
  - dictionary encoding only on the low-cardinality columns brand, color, locale (unique text and
    float vectors are PLAIN instead of starting as a dictionary and falling back);
  - zstd (pyarrow default level), one row group per file.
pyiceberg 0.12 exposes the page size and batch size as table properties but not the page index or
per-column dictionary, so _get_parquet_writer_kwargs is wrapped to add them.
Writes results/iceberg_esci_products_us_pi.json with write time and per-file / per-column page stats
(page counts from walking the Parquet page headers; page index presence from the footer).
"""
import argparse
import json
import statistics
import struct
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyiceberg.io.pyarrow as pyi
from pyiceberg.transforms import IdentityTransform

from build_table import catalog, load_joined, rows_per_file
from common import RESULTS_DIR

DICT_COLS = ["brand", "color", "locale"]
_orig_kwargs = pyi._get_parquet_writer_kwargs


def _kwargs(props):
    kw = _orig_kwargs(props)
    kw["write_page_index"] = True
    kw["use_dictionary"] = DICT_COLS
    return kw


pyi._get_parquet_writer_kwargs = _kwargs


# ---------------------------------------------------------------- page counting (thrift compact)
def _varint(b, i):
    s = r = 0
    while True:
        x = b[i]; i += 1
        r |= (x & 0x7F) << s
        if not x & 0x80:
            return r, i
        s += 7


def _zz(n):
    return (n >> 1) ^ -(n & 1)


def _skip(b, i, t):
    if t in (1, 2):           # bool true/false (in struct field header)
        return i
    if t == 3:                # byte
        return i + 1
    if t in (4, 5, 6):        # i16/i32/i64 varint
        return _varint(b, i)[1]
    if t == 7:                # double
        return i + 8
    if t == 8:                # binary
        n, i = _varint(b, i)
        return i + n
    if t in (9, 10):          # list/set
        h = b[i]; i += 1
        n, et = h >> 4, h & 0x0F
        if n == 15:
            n, i = _varint(b, i)
        for _ in range(n):
            i = i + 1 if et in (1, 2) else _skip(b, i, et)
        return i
    if t == 11:               # map
        n, i = _varint(b, i)
        if n:
            kv = b[i]; i += 1
            for _ in range(n):
                i = _skip(b, i, kv >> 4)
                i = _skip(b, i, kv & 0x0F)
        return i
    if t == 12:               # struct
        return _struct(b, i)[1]
    raise ValueError(f"thrift type {t}")


def _struct(b, i, want=()):
    """Returns ({field_id: i32/i64 value for ids in want}, end offset)."""
    out, fid = {}, 0
    while True:
        h = b[i]; i += 1
        if h == 0:
            return out, i
        d, t = h >> 4, h & 0x0F
        if d:
            fid += d
        else:
            v, i = _varint(b, i); fid = _zz(v)
        if fid in want and t in (4, 5, 6):
            v, i = _varint(b, i); out[fid] = _zz(v)
        else:
            i = _skip(b, i, t)


def count_pages(f, start, total_size):
    """Walks the PageHeaders of one column chunk. Returns (data_pages, dict_pages, data page sizes)."""
    f.seek(start)
    buf = f.read(total_size)
    i, data, dic, sizes = 0, 0, 0, []
    while i < len(buf):
        hdr, j = _struct(buf, i, want=(1, 2, 3))   # type, uncompressed size, compressed size
        if hdr.get(1) == 2:
            dic += 1
        else:
            data += 1
            sizes.append(hdr[2])
        i = j + hdr[3]
    return data, dic, sizes


def inspect(tbl):
    fs = pafs.S3FileSystem(endpoint_override="http://localhost:9020", access_key="minioadmin",
                           secret_key="minioadmin", region="us-east-1", scheme="http")
    files = tbl.inspect.files().to_pandas()
    per_col = {}
    per_file = []
    for path in files.file_path:
        p = path.replace("s3://", "")
        with fs.open_input_file(p) as f:
            md = pq.ParquetFile(f).metadata
            rg = md.row_group(0)
            ent = {"file": path.rsplit("/", 1)[-1], "rows": md.num_rows, "row_groups": md.num_row_groups,
                   "bytes": f.size()}
            for c in range(rg.num_columns):
                cc = rg.column(c)
                start = cc.dictionary_page_offset if cc.has_dictionary_page else cc.data_page_offset
                data, dic, sizes = count_pages(f, start, cc.total_compressed_size)
                pc = per_col.setdefault(cc.path_in_schema, {"data_pages": 0, "dict_pages": 0, "page_bytes": [],
                                                            "has_offset_index": True, "has_column_index": True,
                                                            "compressed_bytes": 0, "encodings": set()})
                pc["data_pages"] += data
                pc["dict_pages"] += dic
                pc["page_bytes"] += sizes
                pc["has_offset_index"] &= cc.has_offset_index
                pc["has_column_index"] &= cc.has_column_index
                pc["compressed_bytes"] += cc.total_compressed_size
                pc["encodings"] |= set(cc.encodings)
            per_file.append(ent)
    cols = {}
    for c, v in per_col.items():
        s = v.pop("page_bytes")
        v["encodings"] = sorted(v["encodings"])
        v["data_page_compressed_bytes_p50"] = int(statistics.median(s)) if s else None
        v["data_page_compressed_bytes_max"] = max(s) if s else None
        cols[c] = v
    return files, per_file, cols


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="esci.products_us_pi")
    ap.add_argument("--target-mb", type=float, default=128)
    ap.add_argument("--page-kb", type=int, default=64)
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--stats-only", action="store_true")
    ap.add_argument("--sort-by", default="", help="skewed layout: sort rows by this column before cutting files")
    a = ap.parse_args()
    cat = catalog()
    out_f = RESULTS_DIR / f"iceberg_{a.table.replace('.', '_')}.json"
    if a.stats_only:
        info = json.loads(out_f.read_text())
    else:
        t_load = time.time()
        t = load_joined()
        load_secs = time.time() - t_load
        if a.sort_by:
            t = t.sort_by([(a.sort_by, "ascending"), ("product_id", "ascending")])
        rpf, per_row = rows_per_file(t, a.target_mb)
        print(f"{t.num_rows} rows, ~{per_row:.0f} B/row -> {rpf} rows/file", flush=True)
        if cat.table_exists(a.table):
            sys.exit(f"{a.table} exists")
        props = {"format-version": "2",
                 "write.parquet.compression-codec": "zstd",
                 "write.target-file-size-bytes": str(4 * 1024 ** 3),
                 "write.parquet.row-group-limit": str(10_000_000),
                 "write.parquet.page-size-bytes": str(a.page_kb * 1024),
                 "write.parquet.page-row-limit": str(a.batch)}
        tbl = cat.create_table(a.table, schema=t.schema, properties=props)
        with tbl.update_spec() as us:
            us.add_field("locale", IdentityTransform(), "locale")
        tbl = cat.load_table(a.table)
        t0 = time.time()
        with tbl.transaction() as tx:
            for s in range(0, t.num_rows, rpf):
                tx.append(t.slice(s, rpf))
                print(f"  appended rows {s}..{min(s + rpf, t.num_rows)} {time.time() - t0:.0f}s", flush=True)
        secs = time.time() - t0
        tbl = cat.load_table(a.table)
        info = {"table": a.table, "source_table_kept": "esci.products_us",
                "metadata_location": tbl.metadata_location, "location": tbl.location(),
                "format_version": tbl.format_version, "partition_spec": str(tbl.spec()),
                "table_properties": props,
                "writer": {"library": "pyiceberg 0.12.0 -> pyarrow ParquetWriter", "compression": "zstd",
                           "data_page_size": a.page_kb * 1024, "write_batch_size": a.batch,
                           "write_page_index": True, "use_dictionary": DICT_COLS,
                           "row_groups_per_file": 1},
                "input_load_secs": round(load_secs, 1), "write_secs": round(secs, 1),
                "rows_per_file_target": rpf,
                "schema": str(tbl.schema())}
    tbl = cat.load_table(a.table)
    files, per_file, cols = inspect(tbl)
    info.update({"rows": int(files.record_count.sum()), "data_files": len(files),
                 "file_mb_min": round(files.file_size_in_bytes.min() / 2 ** 20, 1),
                 "file_mb_median": round(files.file_size_in_bytes.median() / 2 ** 20, 1),
                 "file_mb_max": round(files.file_size_in_bytes.max() / 2 ** 20, 1),
                 "total_gb": round(files.file_size_in_bytes.sum() / 2 ** 30, 2),
                 "page_index_on_every_column_chunk": all(c["has_offset_index"] and c["has_column_index"]
                                                         for c in cols.values()),
                 "columns": cols, "files": per_file,
                 "current_snapshot_id": tbl.current_snapshot().snapshot_id})
    out_f.write_text(json.dumps(info, indent=2))
    print(json.dumps({k: v for k, v in info.items() if k not in ("files", "schema")}, indent=2))


if __name__ == "__main__":
    main()
