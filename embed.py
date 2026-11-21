"""Step 2: bge-base-en-v1.5 embeddings for US products (sharded, resumable) and test queries.

  .venv/bin/python -I embed.py bench [--n 5000]      # throughput + full-set ETA, no output kept
  .venv/bin/python -I embed.py docs                  # writes data/embeddings/shard-00000.parquet ...
  .venv/bin/python -I embed.py queries               # writes data/query_emb_test_us.parquet

Docs are embedded as-is (no instruction); queries get QUERY_INSTRUCTION prepended, as the
model card recommends for short-query -> passage retrieval. All vectors are L2-normalised.
A shard is written atomically (tmp + rename); rerunning skips finished shards.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import (DIM, EMB_DIR, EMBED_MAX_TOKENS, MODEL, PRODUCTS_US, QRELS, QUERY_EMB,
                    QUERY_INSTRUCTION, RESULTS_DIR)

SHARD = 50_000


def load_model(fp16):
    import torch
    from sentence_transformers import SentenceTransformer
    dev = "mps" if torch.backends.mps.is_available() else "cpu"
    m = SentenceTransformer(MODEL, device=dev)
    m.max_seq_length = EMBED_MAX_TOKENS
    if fp16 and dev == "mps":
        m = m.half()
    return m, dev


def encode(m, texts, bs):
    v = m.encode(texts, batch_size=bs, normalize_embeddings=True, convert_to_numpy=True,
                 show_progress_bar=False)
    return v.astype(np.float32)


def vec_table(ids, vecs, id_col):
    flat = pa.array(vecs.reshape(-1), type=pa.float32())
    emb = pa.FixedSizeListArray.from_arrays(flat, DIM)
    return pa.table({id_col: pa.array(ids, pa.string()), "embedding": emb})


def cmd_bench(a):
    texts = pq.read_table(PRODUCTS_US, columns=["text"]).column("text").to_pylist()
    total = len(texts)
    rng = np.random.default_rng(0)
    sample = [texts[i] for i in rng.choice(total, a.n, replace=False)]
    out = {"n": a.n, "total_docs": total, "max_seq_length": EMBED_MAX_TOKENS, "runs": []}
    for fp16 in (False, True):
        m, dev = load_model(fp16)
        encode(m, sample[:256], a.batch)  # warm-up
        t = time.time()
        v = encode(m, sample, a.batch)
        dt = time.time() - t
        r = {"device": dev, "fp16": fp16, "batch": a.batch, "secs": round(dt, 1),
             "docs_per_sec": round(a.n / dt, 1), "eta_full_hours": round(total / (a.n / dt) / 3600, 2)}
        if fp16:
            r["cos_fp16_vs_fp32_min"] = float((v * v32).sum(1).min())
        else:
            v32 = v
        out["runs"].append(r)
        print(json.dumps(r), flush=True)
        del m
    (RESULTS_DIR / "embed_throughput.json").write_text(json.dumps(out, indent=2))


def cmd_docs(a):
    EMB_DIR.mkdir(parents=True, exist_ok=True)
    t = pq.read_table(PRODUCTS_US, columns=["product_id", "text"])
    ids = t.column("product_id").to_pylist()
    texts = t.column("text").to_pylist()
    n = len(ids)
    m, dev = load_model(a.fp16)
    shards = range(0, n, SHARD)
    t0 = time.time(); done_docs = 0
    for s in shards:
        path = EMB_DIR / f"shard-{s // SHARD:05d}.parquet"
        if path.exists():
            continue
        e = min(s + SHARD, n)
        ts = time.time()
        v = encode(m, texts[s:e], a.batch)
        tmp = path.with_suffix(".tmp")
        pq.write_table(vec_table(ids[s:e], v, "product_id"), tmp)
        os.replace(tmp, path)
        done_docs += e - s
        rate = done_docs / (time.time() - t0)
        print(f"{path.name} {e}/{n} {(e - s) / (time.time() - ts):.0f} docs/s, "
              f"remaining ~{(n - e) / rate / 60:.0f} min", flush=True)
    print("docs done", flush=True)


def cmd_queries(a):
    q = pq.read_table(QRELS, columns=["query_id", "query"]).to_pandas().drop_duplicates("query_id")
    m, dev = load_model(a.fp16)
    t = time.time()
    v = encode(m, [QUERY_INSTRUCTION + s for s in q["query"]], a.batch)
    print(f"{len(q)} queries in {time.time() - t:.1f}s")
    tab = vec_table([str(x) for x in q.query_id], v, "query_id").append_column(
        "query", pa.array(q["query"].tolist()))
    pq.write_table(tab, QUERY_EMB)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["bench", "docs", "queries"])
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--batch", type=int, default=128)
    ap.add_argument("--fp16", action="store_true")
    a = ap.parse_args()
    RESULTS_DIR.mkdir(exist_ok=True)
    {"bench": cmd_bench, "docs": cmd_docs, "queries": cmd_queries}[a.cmd](a)


if __name__ == "__main__":
    main()
