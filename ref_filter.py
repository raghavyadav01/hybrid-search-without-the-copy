"""Exact references for the filtered hybrid row: BM25, vector and RRF restricted to products
with a non-null brand (the Pinot hybrid_filter mode uses `brand IS NOT NULL`).

  .venv/bin/python -I ref_filter.py [--skip-bm25] [--skip-vector]

- BM25: same bm25s setup as ref_bm25.py (lucene method, k1=1.2, b=0.75, en stopwords, no stem),
  retrieve(..., weight_mask=has_brand) so only branded products can be returned (exact top-100).
- Vector: exact inner product over L2-normalised bge-base vectors (= cosine), computed in
  blocks over the 25 embedding shards, branded products only, exact top-100.
- Hybrid: RRF k=60 over the two filtered top-100 lists.
Writes results/ref_bm25_brand.json, ref_vector_exact_brand.json, ref_hybrid_rrf_brand.json
(+ per-query parquet, data/runs/*.parquet).
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import DIM, EMB_DIR, PRODUCTS_US, QUERY_EMB
from evallib import fuse_runs, load_qrels, load_run, run_retriever, save_run, write_results

K = 100


def branded_ids():
    t = pq.read_table(PRODUCTS_US, columns=["product_id", "brand"])
    brand = t.column("brand").to_pylist()
    ids = t.column("product_id").to_pylist()
    return ids, np.array([b is not None for b in brand])


def run_bm25():
    import bm25s
    ids, mask = branded_ids()
    texts = pq.read_table(PRODUCTS_US, columns=["text"]).column("text").to_pylist()
    t0 = time.time()
    tok = bm25s.tokenize(texts, stopwords="en", show_progress=False)
    del texts
    ret = bm25s.BM25(method="lucene", k1=1.2, b=0.75)
    ret.index(tok, show_progress=False)
    del tok
    print(f"bm25 index {time.time() - t0:.0f}s, branded {mask.sum()}", flush=True)
    idarr = np.array(ids, dtype=object)
    wmask = mask.astype(np.float32)

    def retriever(chunk):
        q = bm25s.tokenize([x for _, x in chunk], stopwords="en", show_progress=False)
        docs, scores = ret.retrieve(q, k=K, show_progress=False, n_threads=4, weight_mask=wmask)
        return [[idarr[i] for i, s in zip(row, srow) if s > 0 and mask[i]] for row, srow in zip(docs, scores)]

    run_retriever("ref_bm25_brand", retriever,
                  {"engine": f"bm25s {bm25s.__version__}", "method": "lucene", "k1": 1.2, "b": 0.75,
                   "stopwords": "en", "stemmer": None, "filter": "brand IS NOT NULL (weight_mask)", "k": K},
                  batch=1024)


def run_vector():
    ids, mask = branded_ids()
    branded = {p for p, m in zip(ids, mask) if m}
    qt = pq.read_table(QUERY_EMB)
    qids = qt.column("query_id").to_pylist()
    col = qt.column("embedding").combine_chunks()
    Q = col.values.to_numpy(zero_copy_only=False).reshape(-1, DIM).astype(np.float32)
    nq = len(qids)
    best_s = np.full((nq, K), -np.inf, dtype=np.float32)
    best_i = np.full((nq, K), -1, dtype=np.int64)
    all_ids = []
    t0 = time.time()
    for shard in sorted(EMB_DIR.glob("shard-*.parquet")):
        t = pq.read_table(shard)
        pid = t.column("product_id").to_pylist()
        keep = np.array([p in branded for p in pid])
        e = t.column("embedding").combine_chunks()
        X = e.values.to_numpy(zero_copy_only=False).reshape(-1, DIM).astype(np.float32)[keep]
        base = len(all_ids)
        all_ids.extend(p for p, k in zip(pid, keep) if k)
        for a in range(0, nq, 2048):
            s = Q[a:a + 2048] @ X.T
            top = np.argpartition(-s, K - 1, axis=1)[:, :K] if s.shape[1] > K else np.argsort(-s, axis=1)
            ts = np.take_along_axis(s, top, axis=1)
            cs = np.concatenate([best_s[a:a + 2048], ts], axis=1)
            ci = np.concatenate([best_i[a:a + 2048], top + base], axis=1)
            sel = np.argpartition(-cs, K - 1, axis=1)[:, :K]
            best_s[a:a + 2048] = np.take_along_axis(cs, sel, axis=1)
            best_i[a:a + 2048] = np.take_along_axis(ci, sel, axis=1)
        print(f"{shard.name} done {time.time() - t0:.0f}s", flush=True)
    order = np.argsort(-best_s, axis=1)
    best_i = np.take_along_axis(best_i, order, axis=1)
    idarr = np.array(all_ids, dtype=object)
    res = {q: [idarr[i] for i in row if i >= 0] for q, row in zip(qids, best_i)}

    def retriever(chunk):
        return [res[qid] for qid, _ in chunk]

    run_retriever("ref_vector_exact_brand", retriever,
                  {"engine": "numpy exact inner product (cosine on normalised vectors)",
                   "filter": "brand IS NOT NULL", "k": K}, batch=4096)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-bm25", action="store_true")
    ap.add_argument("--skip-vector", action="store_true")
    a = ap.parse_args()
    if not a.skip_vector:
        run_vector()
    if not a.skip_bm25:
        run_bm25()
    _, qrels = load_qrels()
    fused = fuse_runs([load_run("ref_bm25_brand"), load_run("ref_vector_exact_brand")], k=60, depth=K)
    save_run("ref_hybrid_rrf_brand", fused)
    write_results("ref_hybrid_rrf_brand", fused, qrels,
                  {"fusion": "RRF", "rrf_k": 60, "inputs": ["ref_bm25_brand", "ref_vector_exact_brand"],
                   "filter": "brand IS NOT NULL", "input_depth": K, "output_depth": K})


if __name__ == "__main__":
    main()
