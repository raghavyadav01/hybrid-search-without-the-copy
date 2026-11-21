"""Qrels, metrics, RRF and a generic runner for ESCI retrieval runs.

Metric definitions (fixed for the paper):
  nDCG@10   gain(E,S,C,I) = (1.0, 0.1, 0.01, 0); DCG = sum_i gain_i / log2(i + 1), i = 1..10;
            IDCG from the query's judged products sorted by gain; unjudged retrieved products
            have gain 0. Queries with IDCG = 0 are excluded (none in the US test split has only I).
  Recall@100  |retrieved[:100] ∩ Exact| / |Exact|, over queries with >= 1 Exact product.
            Also reported: recall_es@100 with relevant = Exact ∪ Substitute.
Every metric is a macro average over queries.

Self-test:  .venv/bin/python -I evallib.py
"""
import json
import math
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import pyarrow as pa
import pyarrow.parquet as pq

from common import DATA_DIR, GAINS, QRELS, RESULTS_DIR

RUNS_DIR = DATA_DIR / "runs"


# ---------------------------------------------------------------- qrels
def load_qrels(path=QRELS, version="large"):
    """Returns (queries: {qid: text}, qrels: {qid: {product_id: label}}).

    version="large": the full official test split (primary).
    version="small": the Task-1 reduced test set (small_version == 1).
    """
    t = pq.read_table(path).to_pandas()
    if version == "small":
        t = t[t.small_version == 1]
    queries, qrels = {}, defaultdict(dict)
    for qid, q, pid, lab in zip(t.query_id, t["query"], t.product_id, t.esci_label):
        qid = str(qid)
        queries[qid] = q
        qrels[qid][pid] = lab
    return queries, dict(qrels)


# ---------------------------------------------------------------- metrics
def dcg(gains):
    return sum(g / math.log2(i + 2) for i, g in enumerate(gains))


def ndcg_at_k(ranked, judged, k=10):
    gains = [GAINS.get(judged.get(p, "I"), 0.0) if p in judged else 0.0 for p in ranked[:k]]
    ideal = sorted((GAINS[l] for l in judged.values()), reverse=True)[:k]
    idcg = dcg(ideal)
    return None if idcg == 0 else dcg(gains) / idcg


def recall_at_k(ranked, judged, k=100, labels=("E",)):
    rel = {p for p, l in judged.items() if l in labels}
    if not rel:
        return None
    return len(rel.intersection(ranked[:k])) / len(rel)


def evaluate(run, qrels):
    """run: {qid: [product_id, ...]} ranked. Returns (summary, per_query)."""
    per_q = {}
    for qid, judged in qrels.items():
        ranked = run.get(qid, [])
        per_q[qid] = {"ndcg@10": ndcg_at_k(ranked, judged, 10),
                      "recall@100": recall_at_k(ranked, judged, 100, ("E",)),
                      "recall_es@100": recall_at_k(ranked, judged, 100, ("E", "S"))}
    summary = {}
    for m in ("ndcg@10", "recall@100", "recall_es@100"):
        vals = [v[m] for v in per_q.values() if v[m] is not None]
        summary[m] = round(sum(vals) / len(vals), 4)
        summary[f"n_{m}"] = len(vals)
    summary["queries_with_empty_result"] = sum(1 for q in qrels if not run.get(q))
    return summary, per_q


# ---------------------------------------------------------------- fusion
def rrf(rankings, k=60, depth=100):
    """Reciprocal rank fusion: score(d) = sum_r 1 / (k + rank_r(d)), rank starting at 1."""
    s = defaultdict(float)
    for r in rankings:
        for i, d in enumerate(r):
            s[d] += 1.0 / (k + i + 1)
    return [d for d, _ in sorted(s.items(), key=lambda x: (-x[1], x[0]))[:depth]]


def fuse_runs(runs, k=60, depth=100):
    qids = set().union(*runs)
    return {q: rrf([r.get(q, []) for r in runs], k, depth) for q in qids}


# ---------------------------------------------------------------- runner
def save_run(name, run):
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    q, p, r = [], [], []
    for qid, lst in run.items():
        for i, pid in enumerate(lst):
            q.append(qid); p.append(pid); r.append(i + 1)
    pq.write_table(pa.table({"query_id": q, "product_id": p, "rank": pa.array(r, pa.int16())}),
                   RUNS_DIR / f"{name}.parquet")


def load_run(name):
    t = pq.read_table(RUNS_DIR / f"{name}.parquet").to_pandas().sort_values(["query_id", "rank"])
    run = defaultdict(list)
    for q, p in zip(t.query_id, t.product_id):
        run[q].append(p)
    return dict(run)


def write_results(name, run, qrels, config, timing=None, version="large"):
    summary, per_q = evaluate(run, qrels)
    out = {"name": name, "qrels": f"esci-us-test-{version}", "config": config,
           "metrics": summary, "timing": timing or {},
           "written_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"{name}.json").write_text(json.dumps(out, indent=2))
    pq.write_table(pa.table({
        "query_id": list(per_q),
        "ndcg_at_10": [v["ndcg@10"] for v in per_q.values()],
        "recall_at_100": [v["recall@100"] for v in per_q.values()],
        "recall_es_at_100": [v["recall_es@100"] for v in per_q.values()]}),
        RESULTS_DIR / f"{name}.per_query.parquet")
    print(json.dumps({"name": name, **summary}), flush=True)
    return out


def run_retriever(name, retriever, config, k=100, batch=256, version="large", save=True):
    """retriever(list[(qid, query_text)]) -> list[list[product_id]] (ranked, up to k each).

    Calls the retriever in batches over every test query, records wall time, evaluates and
    writes results/<name>.json + results/<name>.per_query.parquet (+ data/runs/<name>.parquet).
    """
    queries, qrels = load_qrels(version=version)
    items = list(queries.items())
    run, t0, lat = {}, time.time(), []
    for i in range(0, len(items), batch):
        chunk = items[i:i + batch]
        ts = time.time()
        res = retriever(chunk)
        lat.append((time.time() - ts) / len(chunk))
        for (qid, _), r in zip(chunk, res):
            run[qid] = list(r)[:k]
    timing = {"wall_secs": round(time.time() - t0, 1), "queries": len(items),
              "mean_secs_per_query": round(sum(lat) / len(lat), 5)}
    if save:
        save_run(name, run)
    return write_results(name, run, qrels, config, timing, version)


# ---------------------------------------------------------------- self-test
def _selftest():
    j = {"a": "E", "b": "S", "c": "C", "d": "I", "e": "E"}
    # ideal order: E, E, S, C, I
    ideal = dcg([1.0, 1.0, 0.1, 0.01, 0.0])
    assert abs(ndcg_at_k(["a", "e", "b", "c", "d"], j) - 1.0) < 1e-12
    got = ndcg_at_k(["x", "a", "b"], j)                 # unjudged x at rank 1
    want = (1.0 / math.log2(3) + 0.1 / math.log2(4)) / ideal
    assert abs(got - want) < 1e-12, (got, want)
    assert ndcg_at_k([], j) == 0.0
    assert ndcg_at_k(["a"], {"z": "I"}) is None       # IDCG 0 -> excluded
    # cutoff: an Exact at rank 11 does not count
    assert ndcg_at_k(["x"] * 10 + ["a"], {"a": "E"}) == 0.0
    assert recall_at_k(["a", "x"], j) == 0.5
    assert recall_at_k(["a", "b", "e"], j, labels=("E", "S")) == 1.0
    assert recall_at_k(["x"] * 100 + ["a"], j) == 0.0  # beyond 100
    assert recall_at_k(["a"], {"b": "S"}) is None
    # RRF
    f = rrf([["a", "b", "c"], ["b", "c", "d"]], k=60)
    assert f[0] == "b" and f[1] == "c" and set(f) == {"a", "b", "c", "d"}
    assert abs((1 / 61 + 1 / 62) - (1 / 62 + 1 / 61)) < 1e-15
    s, _ = evaluate({"q1": ["a", "e"], "q2": []}, {"q1": j, "q2": {"z": "E"}})
    assert s["recall@100"] == 0.5 and s["queries_with_empty_result"] == 1
    assert abs(s["ndcg@10"] - (ndcg_at_k(["a", "e"], j) + 0) / 2) < 1e-4
    print("evallib self-test OK")


if __name__ == "__main__":
    _selftest()
