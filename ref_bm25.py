"""BM25 reference over the US product `text` column (bm25s, in-memory, exact top-k).

  .venv/bin/python -I ref_bm25.py [--stem]

Lucene-style BM25 (method="lucene", k1=1.2, b=0.75 = Lucene BM25Similarity defaults),
lowercase + bm25s default token pattern, English stopwords. Default: no stemming (closest to
a Lucene StandardAnalyzer text index); --stem adds the English Snowball stemmer.
Writes results/ref_bm25[_stem].json and data/runs/ref_bm25[_stem].parquet.
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import bm25s
import numpy as np
import pyarrow.parquet as pq

from common import PRODUCTS_US
from evallib import run_retriever

K1, B = 1.2, 0.75


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stem", action="store_true")
    a = ap.parse_args()
    stemmer = None
    if a.stem:
        import Stemmer
        stemmer = Stemmer.Stemmer("english")
    name = "ref_bm25_stem" if a.stem else "ref_bm25"

    t = pq.read_table(PRODUCTS_US, columns=["product_id", "text"])
    ids = np.array(t.column("product_id").to_pylist(), dtype=object)
    texts = t.column("text").to_pylist()
    t0 = time.time()
    corpus_tok = bm25s.tokenize(texts, stopwords="en", stemmer=stemmer, show_progress=False)
    del texts
    ret = bm25s.BM25(method="lucene", k1=K1, b=B)
    ret.index(corpus_tok, show_progress=False)
    del corpus_tok
    index_secs = time.time() - t0
    print(f"indexed {len(ids)} docs in {index_secs:.0f}s", flush=True)

    def retriever(chunk):
        q_tok = bm25s.tokenize([q for _, q in chunk], stopwords="en", stemmer=stemmer,
                               show_progress=False)
        docs, _ = ret.retrieve(q_tok, k=100, show_progress=False, n_threads=8)
        return [[ids[i] for i in row] for row in docs]

    cfg = {"engine": f"bm25s {bm25s.__version__}", "method": "lucene", "k1": K1, "b": B,
           "stopwords": "en", "stemmer": "snowball-english" if a.stem else None,
           "field": "text", "k": 100, "index_secs": round(index_secs, 1)}
    run_retriever(name, retriever, cfg, batch=1024)


if __name__ == "__main__":
    main()
