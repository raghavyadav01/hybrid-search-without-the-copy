"""Exact (brute-force) vector reference: inner product over L2-normalised bge-base vectors
(= cosine), faiss IndexFlatIP, top-100 for every US test query.

  .venv/bin/python -I ref_vector.py
Needs data/embeddings/shard-*.parquet (embed.py docs) and data/query_emb_test_us.parquet
(embed.py queries). Writes results/ref_vector_exact.json and data/runs/ref_vector_exact.parquet.
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import faiss
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import DIM, EMB_DIR, MODEL, QUERY_EMB, QUERY_INSTRUCTION
from evallib import run_retriever


def to_matrix(col):
    col = col.combine_chunks()
    vals = col.values if isinstance(col, pa.FixedSizeListArray) else col.flatten()
    return vals.to_numpy(zero_copy_only=False).reshape(-1, DIM).astype(np.float32, copy=False)


def main():
    shards = sorted(EMB_DIR.glob("shard-*.parquet"))
    t = pa.concat_tables([pq.read_table(s) for s in shards])
    ids = np.array(t.column("product_id").to_pylist(), dtype=object)
    x = np.ascontiguousarray(to_matrix(t.column("embedding")))
    del t
    t0 = time.time()
    index = faiss.IndexFlatIP(DIM)
    index.add(x)
    print(f"{index.ntotal} vectors indexed in {time.time() - t0:.1f}s", flush=True)

    qt = pq.read_table(QUERY_EMB)
    qvec = dict(zip(qt.column("query_id").to_pylist(), to_matrix(qt.column("embedding"))))

    def retriever(chunk):
        q = np.stack([qvec[qid] for qid, _ in chunk])
        _, idx = index.search(q, 100)
        return [[ids[i] for i in row if i >= 0] for row in idx]

    cfg = {"engine": f"faiss {faiss.__version__} IndexFlatIP (exact)", "model": MODEL,
           "dim": DIM, "metric": "inner product on L2-normalised vectors (cosine)",
           "query_instruction": QUERY_INSTRUCTION, "k": 100, "docs": int(len(ids))}
    run_retriever("ref_vector_exact", retriever, cfg, batch=1024)


if __name__ == "__main__":
    main()
