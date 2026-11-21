"""results/storage.md: bytes per system for the ESCI US products, and the extra bytes each system keeps
beyond the lake (the Iceberg Parquet files, which every system reads).

  .venv/bin/python -I make_storage.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

from common import RESULTS_DIR


def load(n):
    f = RESULTS_DIR / f"{n}.json"
    return json.loads(f.read_text()) if f.exists() else None


def g(b):
    return "-" if b is None else f"{b / 1e9:.2f} GB"


def main():
    ice = load("iceberg_esci_products_us_pi")
    lake = sum(f["bytes"] for f in ice["files"])
    rows = []   # (system, text, vector, source/forward, other, total, extra)
    rows.append(("Lake: Iceberg Parquet `esci.products_us_pi` (33 files, zstd)", None, None, lake, None, lake, 0))
    et = load("pinot_et_timings")
    if et:
        b = et["esci_et"]["index_sizes"]["bytes_by_index_type"]
        t, v = b.get("text_index", 0), b.get("vector_index", 0)
        o = sum(x for k, x in b.items() if k not in ("text_index", "vector_index"))
        rows.append(("Pinot External Table (server-side: indexes only; data read in place)", t, v, 0, o, t + v + o, t + v + o))
    lo = load("pinot_local_timings")
    if lo:
        b = lo["esci_local"]["index_sizes"]["bytes_by_index_type"]
        t, v, f = b.get("text_index", 0), b.get("vector_index", 0), b.get("forward_index", 0)
        o = sum(x for k, x in b.items() if k not in ("text_index", "vector_index", "forward_index"))
        rows.append(("Pinot local copy (segments: forward + indexes)", t, v, f, o, t + v + f + o, t + v + f + o))
    for eng, name in (("es", "Elasticsearch"), ("os", "OpenSearch")):
        tm = load(f"{eng}_timings")
        if not tm:
            continue
        sz = tm["index_size"]
        store = sz["store_bytes"]
        t = sz.get("es_disk_usage_text_bytes")
        v = sz.get("es_disk_usage_embedding_bytes")
        if v is None and sz.get("split"):
            vals = list(sz["split"].values())
            v, t = vals[0], vals[1]
        rows.append((f"{name} {tm['version']} (1 shard, force-merged, `_source` disabled)", t, v, 0,
                     store - (t or 0) - (v or 0), store, store))
    out = ["# Storage — bytes per system (ESCI US, 1,215,854 products, 768-d float32 vectors)", ""]
    out.append("| System | Text index | Vector index | Row data (forward / `_source`) | Other | Total | Extra bytes beyond the lake |")
    out.append("|---|---|---|---|---|---|---|")
    for r in rows:
        out.append(f"| {r[0]} | {g(r[1])} | {g(r[2])} | {g(r[3])} | {g(r[4])} | {g(r[5])} | {g(r[6])} |")
    out.append("")
    out.append("- The lake is shared: every system reads these Parquet files; only Pinot ET serves queries from them in "
               "place, so its extra bytes are just the text + HNSW indexes (+ null vectors, metadata).")
    out.append("- Pinot local: per-index bytes summed over all segments from the servers' segment-metadata API "
               "(forward index = raw/dictionary columns incl. the vectors).")
    out.append("- ES: per-field bytes from `_disk_usage` (text = the `text` field, vector = `embedding`); OS: by Lucene "
               "file extension (vec/vex/vem* = vector, rest = text postings, norms, ids, keywords). `_source` is "
               "disabled in both (a production deployment that keeps `_source` stores the row data again: add "
               "about the size of the JSON documents). 'Other' = store minus text minus vector.")
    out.append("- GB = 10^9 bytes.")
    (RESULTS_DIR / "storage.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
