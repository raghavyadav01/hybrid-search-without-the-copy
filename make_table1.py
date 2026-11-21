"""Builds results/table1.md (ISIR Table 1: retrieval quality) from the sequential, one-system-at-a-time
runs over the full ESCI US test split (22,458 queries).

  .venv/bin/python -I make_table1.py

Every number is a macro average over the same query set, from the saved per-query metrics
(results/<run>.per_query.parquet). Significance: paired two-sided randomisation (sign-flip) test on
the per-query nDCG@10 differences, 10,000 permutations, plus a paired t-test (scipy).
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow.parquet as pq
from scipy import stats as sstats

from common import RESULTS_DIR
from evallib import load_qrels

PERMS, SEED = 10_000, 2026


def per_query(name):
    p = RESULTS_DIR / f"{name}.per_query.parquet"
    if not p.exists():
        return None
    return pq.read_table(p).to_pandas().set_index("query_id")


def summary(name, qids):
    t = per_query(name)
    if t is None:
        return None
    t = t.loc[t.index.intersection(qids)]
    if len(t) < len(qids):
        return None
    return float(t.ndcg_at_10.dropna().mean()), float(t.recall_at_100.dropna().mean()), len(t)


def paired(a, b, qids):
    ta, tb = per_query(a), per_query(b)
    if ta is None or tb is None:
        return None
    x = ta.ndcg_at_10.reindex(qids).to_numpy(float)
    y = tb.ndcg_at_10.reindex(qids).to_numpy(float)
    m = ~(np.isnan(x) | np.isnan(y))
    d = x[m] - y[m]
    obs = d.mean()
    rng = np.random.default_rng(SEED)
    cnt = 0
    for _ in range(PERMS // 1000):
        signs = rng.choice((-1.0, 1.0), size=(1000, len(d)))
        cnt += int((np.abs((signs * d).mean(axis=1)) >= abs(obs) - 1e-15).sum())
    p_rand = (cnt + 1) / (PERMS + 1)
    p_t = float(sstats.ttest_rel(x[m], y[m]).pvalue)
    return {"a": a, "b": b, "n": int(m.sum()), "mean_a": float(x[m].mean()), "mean_b": float(y[m].mean()),
            "diff": float(obs), "p_randomisation": p_rand, "p_ttest": p_t,
            "wins_a": int((d > 0).sum()), "wins_b": int((d < 0).sum()), "ties": int((d == 0).sum())}


def overlap(a, b):
    from evallib import RUNS_DIR, load_run
    if not (RUNS_DIR / f"{a}.parquet").exists() or not (RUNS_DIR / f"{b}.parquet").exists():
        return None
    ra, rb = load_run(a), load_run(b)
    vals = [len(set(ra.get(q, [])[:100]) & set(rb[q][:100])) / max(1, min(100, len(rb[q])))
            for q in rb if rb[q]]
    return float(np.mean(vals))


ROWS = [
    ("Reference", "BM25 (bm25s, exact, global stats)", "ref_bm25"),
    ("Reference", "Vector (exact cosine, faiss flat)", "ref_vector_exact"),
    ("Reference", "Hybrid RRF k=60 (client, exact lists)", "ref_hybrid_rrf_bm25"),
    ("Reference", "Hybrid RRF + filter (brand IS NOT NULL, exact)", "ref_hybrid_rrf_brand"),
    ("Pinot External Table", "BM25, per-segment IDF (TEXT_SCORE)", "pinot_et_bm25"),
    ("Pinot External Table", "BM25, global stats (TEXT_SCORE_STATS + TEXT_SCORE)", "pinot_et_bm25_global"),
    ("Pinot External Table", "Vector (HNSW, k=100/segment)", "pinot_et_vector"),
    ("Pinot External Table", "Hybrid RRF k=60, multi-stage SQL (global BM25)", "pinot_et_hybrid_sql"),
    ("Pinot External Table", "Hybrid RRF k=60, multi-stage SQL (per-segment BM25)", "pinot_et_hybrid_sql_perseg"),
    ("Pinot External Table", "Hybrid RRF k=60, client-side (global BM25 + vector runs)", "pinot_et_hybrid_client"),
    ("Pinot External Table", "Hybrid RRF + filter (brand IS NOT NULL), SQL", "pinot_et_hybrid_filter"),
    ("Pinot local copy", "BM25, per-segment IDF (TEXT_SCORE)", "pinot_local_bm25"),
    ("Pinot local copy", "BM25, global stats (TEXT_SCORE_STATS + TEXT_SCORE)", "pinot_local_bm25_global"),
    ("Pinot local copy", "Vector (HNSW, k=100/segment)", "pinot_local_vector"),
    ("Pinot local copy", "Hybrid RRF k=60, multi-stage SQL (global BM25)", "pinot_local_hybrid_sql"),
    ("Pinot local copy", "Hybrid RRF k=60, multi-stage SQL (per-segment BM25)", "pinot_local_hybrid_sql_perseg"),
    ("Pinot local copy", "Hybrid RRF k=60, client-side", "pinot_local_hybrid_client"),
    ("Pinot local copy", "Hybrid RRF + filter (brand IS NOT NULL), SQL", "pinot_local_hybrid_filter"),
    ("Elasticsearch", "BM25 (match)", "es_bm25"),
    ("Elasticsearch", "kNN (HNSW, num_candidates=200)", "es_knn"),
    ("Elasticsearch", "Hybrid RRF k=60, native retriever.rrf", "es_hybrid_native"),
    ("Elasticsearch", "Hybrid RRF k=60, client-side", "es_hybrid_client"),
    ("Elasticsearch", "kNN (HNSW, num_candidates=1000)", "es_knn_c1000"),
    ("Elasticsearch", "Hybrid RRF k=60, native retriever.rrf (num_candidates=1000)", "es_hybrid_native_c1000"),
    ("Elasticsearch", "Hybrid RRF k=60, client-side (num_candidates=1000)", "es_hybrid_client_c1000"),
    ("OpenSearch", "BM25 (match)", "os_bm25"),
    ("OpenSearch", "kNN (HNSW lucene, ef_search=200)", "os_knn"),
    ("OpenSearch", "Hybrid RRF k=60, native score-ranker", "os_hybrid_native"),
    ("OpenSearch", "Hybrid RRF k=60, client-side", "os_hybrid_client"),
    ("OpenSearch", "kNN (HNSW lucene, ef_search=1000)", "os_knn_c1000"),
    ("OpenSearch", "Hybrid RRF k=60, native score-ranker (ef_search=1000)", "os_hybrid_native_c1000"),
    ("OpenSearch", "Hybrid RRF k=60, client-side (ef_search=1000)", "os_hybrid_client_c1000"),
]

TESTS = [
    ("Pinot ET hybrid SQL vs Elasticsearch native RRF", "pinot_et_hybrid_sql", "es_hybrid_native"),
    ("Pinot ET hybrid SQL vs OpenSearch native RRF", "pinot_et_hybrid_sql", "os_hybrid_native"),
    ("Pinot ET hybrid SQL vs Elasticsearch client RRF", "pinot_et_hybrid_sql", "es_hybrid_client"),
    ("Pinot ET hybrid SQL vs OpenSearch client RRF", "pinot_et_hybrid_sql", "os_hybrid_client"),
    ("Pinot ET vs local: BM25 per-segment", "pinot_et_bm25", "pinot_local_bm25"),
    ("Pinot ET vs local: BM25 global", "pinot_et_bm25_global", "pinot_local_bm25_global"),
    ("Pinot ET vs local: vector", "pinot_et_vector", "pinot_local_vector"),
    ("Pinot ET vs local: hybrid SQL", "pinot_et_hybrid_sql", "pinot_local_hybrid_sql"),
    ("Pinot ET vs local: hybrid SQL per-segment", "pinot_et_hybrid_sql_perseg", "pinot_local_hybrid_sql_perseg"),
    ("Pinot ET vs local: hybrid + filter", "pinot_et_hybrid_filter", "pinot_local_hybrid_filter"),
    ("Pinot ET BM25 global vs per-segment", "pinot_et_bm25_global", "pinot_et_bm25"),
    ("Pinot ET hybrid SQL global vs per-segment BM25", "pinot_et_hybrid_sql", "pinot_et_hybrid_sql_perseg"),
    ("Pinot ET hybrid + filter vs filtered exact reference", "pinot_et_hybrid_filter", "ref_hybrid_rrf_brand"),
    ("Pinot ET hybrid SQL vs exact reference hybrid", "pinot_et_hybrid_sql", "ref_hybrid_rrf_bm25"),
    ("Pinot ET BM25 global vs Elasticsearch BM25", "pinot_et_bm25_global", "es_bm25"),
    ("Pinot ET vector vs Elasticsearch kNN", "pinot_et_vector", "es_knn"),
    ("Pinot ET vector vs OpenSearch kNN", "pinot_et_vector", "os_knn"),
    ("Pinot ET hybrid SQL vs Elasticsearch native RRF (num_candidates=1000)", "pinot_et_hybrid_sql", "es_hybrid_native_c1000"),
    ("Pinot ET hybrid SQL vs OpenSearch native RRF (ef_search=1000)", "pinot_et_hybrid_sql", "os_hybrid_native_c1000"),
    ("Pinot ET hybrid SQL vs Elasticsearch client RRF (num_candidates=1000)", "pinot_et_hybrid_sql", "es_hybrid_client_c1000"),
    ("Pinot ET hybrid SQL vs OpenSearch client RRF (ef_search=1000)", "pinot_et_hybrid_sql", "os_hybrid_client_c1000"),
    ("Pinot ET vector vs Elasticsearch kNN (num_candidates=1000)", "pinot_et_vector", "es_knn_c1000"),
    ("Pinot ET vector vs OpenSearch kNN (ef_search=1000)", "pinot_et_vector", "os_knn_c1000"),
]


def main():
    queries, _ = load_qrels()
    qids = sorted(queries)
    out = ["# Table 1 — ESCI (US) retrieval quality: lake-native vs local copy vs search engines", ""]
    out.append(f"All {len(qids):,} queries of the ESCI US test split; every row is averaged over exactly these "
               "queries. nDCG@10 with gains E=1, S=0.1, C=0.01, I=0; R@100 = recall of Exact products in "
               "the top 100. Every system ran alone on the laptop (see timings.md for hardware), all reading "
               "the same 1,215,854 products and bge-base-en-v1.5 vectors from the Iceberg table "
               "`esci.products_us_pi`.")
    out.append("")
    out.append("| System | Mode | nDCG@10 | R@100 | run |")
    out.append("|---|---|---|---|---|")
    missing = []
    for sysname, label, run in ROWS:
        s = summary(run, qids)
        if s is None:
            missing.append(run)
            out.append(f"| {sysname} | {label} | n/a | n/a | `{run}` (missing) |")
        else:
            out.append(f"| {sysname} | {label} | {s[0]:.4f} | {s[1]:.4f} | `{run}` |")
    out.append("")
    out.append("## Paired significance (per-query nDCG@10)")
    out.append("")
    out.append(f"Two-sided paired randomisation test (sign flip, {PERMS:,} permutations, seed {SEED}) and paired "
               "t-test over the per-query nDCG@10 of the two runs.")
    out.append("")
    out.append("| Comparison | A | B | Δ (A−B) | p (randomisation) | p (t-test) | A wins / B wins / ties |")
    out.append("|---|---|---|---|---|---|---|")
    sig = []
    for label, a, b in TESTS:
        r = paired(a, b, qids)
        if r is None:
            out.append(f"| {label} | n/a | n/a | n/a | n/a | n/a | n/a |")
            continue
        sig.append({"label": label, **r})
        pr = "<0.0001" if r["p_randomisation"] < 1e-4 else f"{r['p_randomisation']:.4f}"
        out.append(f"| {label} | {r['mean_a']:.4f} | {r['mean_b']:.4f} | {r['diff']:+.4f} | {pr} | "
                   f"{r['p_ttest']:.2e} | {r['wins_a']} / {r['wins_b']} / {r['ties']} |")
    out.append("")
    out.append("## Approximate kNN vs the exact reference (matched recall)")
    out.append("")
    out.append("Top-100 overlap with the exact cosine top-100 (`ref_vector_exact`), averaged per query, and R@100 "
               "of each run next to the exact R@100.")
    out.append("")
    out.append("| Run | R@100 | exact R@100 | ΔR@100 | mean overlap with exact top-100 |")
    out.append("|---|---|---|---|---|")
    ex = summary("ref_vector_exact", qids)
    for run in ("pinot_et_vector", "pinot_local_vector", "es_knn", "es_knn_c1000", "os_knn", "os_knn_c1000"):
        sm, ov = summary(run, qids), overlap(run, "ref_vector_exact")
        if sm and ex:
            out.append(f"| `{run}` | {sm[1]:.4f} | {ex[1]:.4f} | {sm[1] - ex[1]:+.4f} | "
                       f"{'n/a' if ov is None else f'{ov:.3f}'} |")
    out.append("")
    out.append("## Notes")
    out.append("")
    out.append("- Pinot BM25 = Lucene BM25Similarity (k1=1.2, b=0.75), StandardAnalyzer + 33 English stop "
               "words, no stemming; ES/OS = `standard` + `_english_` stop words, the same BM25. Per-segment "
               "IDF: each Pinot segment (one per Parquet file, 33) is its own Lucene index; global stats: "
               "two-step `TEXT_SCORE_STATS` then 4-argument `TEXT_SCORE` (text-score-design.md §7), latency "
               "includes both requests.")
    out.append("- Pinot vector: HNSW (M=16, beamWidth=100), cosine, top-100 per segment, vectorEfSearch "
               "unset; ES/OS: HNSW m=16 ef_construction=100, float32, num_candidates/ef_search=200, one shard.")
    out.append("- Pinot hybrid SQL: RRF k=60 over the two top-100 lists in one multi-stage query; the filter "
               "row applies `brand IS NOT NULL` inside both retrievers (enableNullHandling=true). Global "
               "BM25 statistics ignore the filter (as ES DFS does).")
    out.append("- References: bm25s (exact BM25, one global index) and exact cosine top-100; filtered "
               "references restrict both to branded products (ref_filter.py).")
    (RESULTS_DIR / "table1.md").write_text("\n".join(out) + "\n")
    (RESULTS_DIR / "table1_significance.json").write_text(json.dumps(sig, indent=2))
    print("\n".join(out))
    if missing:
        print("MISSING:", missing)


if __name__ == "__main__":
    main()
