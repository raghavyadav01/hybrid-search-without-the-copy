"""Builds results/table1.md (ISIR Table 1) from the Pinot runs and the reference runs.

  .venv/bin/python -I make_table1.py

Every number in a row is computed over the SAME query set, from the saved per-query metrics
(results/<run>.per_query.parquet), so lake-native, local-copy and reference are apples-to-apples.
Main table: the 2,000-query sample of the Task-1 small split (seed 2026) used for the vector and
hybrid modes. Extra tables: BM25 on the whole small split and on the full test split.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import pyarrow.parquet as pq

from common import RESULTS_DIR
from pinot_runs import load_qrels

SAMPLE, SEED = 2000, 2026


def per_query(name):
    p = RESULTS_DIR / f"{name}.per_query.parquet"
    if not p.exists():
        return None
    t = pq.read_table(p).to_pandas().set_index("query_id")
    return t


def mean_over(name, qids):
    t = per_query(name)
    if t is None:
        return None
    t = t.loc[t.index.intersection(qids)]
    if len(t) < len(qids):
        return None  # run does not cover this query set
    nd = t.ndcg_at_10.dropna()
    rc = t.recall_at_100.dropna()
    return round(float(nd.mean()), 4), round(float(rc.mean()), 4), len(nd)


def fmt(m):
    return ("{:.4f}".format(m[0]), "{:.4f}".format(m[1])) if m else ("n/a", "n/a")


def latency(name):
    p = RESULTS_DIR / f"{name}.json"
    if not p.exists():
        return None
    d = json.loads(p.read_text()).get("latency")
    return d


def first(*names):
    for n in names:
        if (RESULTS_DIR / f"{n}.per_query.parquet").exists():
            return n
    return names[0]


def main():
    sample_q = sorted(load_qrels("small", SAMPLE, SEED)[0])
    small_q = sorted(load_qrels("small")[0])
    large_q = sorted(load_qrels("large")[0])

    rows = [
        ("BM25 (TEXT_SCORE)", "bm25", "ref_bm25"),
        ("Vector (HNSW, k=100/segment)", "vector", "ref_vector_exact"),
        ("Hybrid RRF k=60 (multi-stage SQL)", "hybrid_sql", "ref_hybrid_rrf_bm25"),
        ("Hybrid RRF k=60 (client-side over the two lists)", "hybrid_client", "ref_hybrid_rrf_bm25"),
        ("Hybrid RRF + filter (brand IS NOT NULL)", "hybrid_filter", "ref_hybrid_rrf_brand"),
    ]

    def run_name(table, mode):
        return first(f"pinot_{table}_{mode}_small_s{SAMPLE}", f"pinot_{table}_{mode}_small",
                     f"pinot_{table}_{mode}")

    out = ["# Table 1 — ESCI (US) retrieval quality: lake-native vs local copy vs reference", ""]
    out.append(f"Query set: {len(sample_q):,}-query random sample (seed {SEED}) of the Task-1 small test split "
               "(8,956 queries); every column of a row is averaged over exactly these queries. "
               "nDCG@10 with gains E=1, S=0.1, C=0.01, I=0; R@100 = recall of Exact products in the top 100.")
    out.append("")
    out.append("| Mode | Lake-native (External Table) nDCG@10 | R@100 | Local copy nDCG@10 | R@100 "
               "| Reference nDCG@10 | R@100 |")
    out.append("|---|---|---|---|---|---|---|")
    for label, mode, ref in rows:
        et = mean_over(run_name("et", mode), sample_q)
        lc = mean_over(run_name("local", mode), sample_q)
        rf = mean_over(ref, sample_q)
        out.append(f"| {label} | {fmt(et)[0]} | {fmt(et)[1]} | {fmt(lc)[0]} | {fmt(lc)[1]} | "
                   f"{fmt(rf)[0]} | {fmt(rf)[1]} |")
    out.append("")
    out.append("Reference systems: BM25 = bm25s (Lucene BM25 k1=1.2 b=0.75, one global index, English "
               "stop words, no stemming); Vector = exact cosine (faiss IndexFlatIP on the same normalised "
               "bge-base vectors); Hybrid = client-side RRF k=60 over the two exact top-100 lists; "
               "filtered reference = the same three computed over branded products only (ref_filter.py).")
    out.append("")

    # BM25 on larger query sets
    out.append("## BM25 on larger query sets")
    out.append("")
    out.append("| Query set | Lake-native nDCG@10 | R@100 | Local copy nDCG@10 | R@100 | Reference nDCG@10 | R@100 |")
    out.append("|---|---|---|---|---|---|---|")
    for label, qs in ((f"Task-1 small split ({len(small_q):,})", small_q),
                      (f"Full test split ({len(large_q):,})", large_q)):
        et = mean_over(first("pinot_et_bm25_small", "pinot_et_bm25"), qs)
        lc = mean_over(first("pinot_local_bm25", "pinot_local_bm25_small"), qs)
        rf = mean_over("ref_bm25", qs)
        out.append(f"| {label} | {fmt(et)[0]} | {fmt(et)[1]} | {fmt(lc)[0]} | {fmt(lc)[1]} | "
                   f"{fmt(rf)[0]} | {fmt(rf)[1]} |")
    out.append("")

    # latency
    out.append("## Warm per-query latency (from the runs, concurrency 8, first 200 requests excluded)")
    out.append("")
    out.append("| Mode | Lake-native p50 / p95 (ms) | Local copy p50 / p95 (ms) |")
    out.append("|---|---|---|")
    for label, mode, _ in rows:
        if mode == "hybrid_client":
            continue
        cells = []
        for table in ("et", "local"):
            d = latency(run_name(table, mode))
            cells.append(f"{d['client_ms_p50']:.0f} / {d['client_ms_p95']:.0f}" if d else "n/a")
        out.append(f"| {label} | {cells[0]} | {cells[1]} |")
    out.append("")
    (RESULTS_DIR / "table1.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
