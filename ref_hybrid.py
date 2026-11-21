"""Client-side hybrid reference: RRF (k=60) over saved BM25 and exact-vector top-100 runs.

  .venv/bin/python -I ref_hybrid.py [--bm25 ref_bm25] [--vector ref_vector_exact] [--k 60]
Writes results/ref_hybrid_rrf_<bm25>.json and data/runs/ref_hybrid_rrf_<bm25>.parquet.
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

from evallib import fuse_runs, load_qrels, load_run, save_run, write_results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bm25", default="ref_bm25")
    ap.add_argument("--vector", default="ref_vector_exact")
    ap.add_argument("--k", type=int, default=60)
    a = ap.parse_args()
    _, qrels = load_qrels()
    t0 = time.time()
    fused = fuse_runs([load_run(a.bm25), load_run(a.vector)], k=a.k, depth=100)
    name = f"ref_hybrid_rrf_{a.bm25.removeprefix('ref_')}"
    save_run(name, fused)
    write_results(name, fused, qrels,
                  {"fusion": "RRF", "rrf_k": a.k, "inputs": [a.bm25, a.vector],
                   "input_depth": 100, "output_depth": 100},
                  {"fuse_secs": round(time.time() - t0, 1)})


if __name__ == "__main__":
    main()
