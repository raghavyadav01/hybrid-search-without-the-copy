"""ESCI quality runs against Pinot (StarTree External Table and a local-copy OFFLINE table).

  .venv/bin/python -I pinot_runs.py --table et|local --mode MODE [--version large|small]
                                    [--sample N --seed 2026] [--concurrency 8] [--limit N] [--broker URL]

Tables (created per stp-build.md, "ESCI tables"): esci_et (External Table over the Iceberg table
esci.products_us on MinIO; Lucene text index on `text`, HNSW on `embedding`) and esci_local
(OFFLINE table ingested from the same 33 Parquet files, same indexes).

Modes (each list is top-100; queries go to the broker REST API /query/sql):
  bm25            SELECT product_id, TEXT_SCORE(text, '<q>') AS s FROM t WHERE TEXT_MATCH(text, '<q>')
                  ORDER BY s DESC LIMIT 100                                    (single-stage engine)
  vector          SELECT product_id, cosineDistance(embedding, ARRAY[v]) AS d FROM t
                  WHERE VECTOR_SIMILARITY(embedding, ARRAY[v], 100) ORDER BY d LIMIT 100
                  vectorEfSearch is NOT set: in this build it is a cap on visited graph nodes
                  (TopKnnCollector visitLimit), not the HNSW ef queue; =200 cut overlap with the
                  exact top-100 to ~55%. Unset, Lucene searches each segment with beam = k = 100
                  (~95-97% overlap with exact top-100 on a 48-query probe)     (single-stage engine)
  hybrid_sql      RRF k=60 in multi-stage SQL (text-score-design.md section 3): two CTEs with
                  ROW_NUMBER() over the BM25 and the vector top-100, UNION ALL + GROUP BY,
                  SUM(1.0/(60 + r)) ORDER BY rrf DESC, product_id LIMIT 100. The inner top-100
                  queries order by the score only (no product_id tie-breaker): a second sort key makes
                  Pinot read product_id for every matching row (2x cost on the External Table); ties
                  are broken by product_id inside ROW_NUMBER()
  hybrid_client   client-side RRF (evallib.fuse_runs, k=60) over this table's saved bm25 and vector
                  runs (no queries)
  hybrid_filter   hybrid_sql with the structured filter FILTER_SQL added to BOTH branches (so the
                  filter is applied inside each retriever: TEXT_MATCH AND filter, and
                  VECTOR_SIMILARITY AND filter)

Lucene query text (exactly): the raw ESCI query is lower-cased (so AND/OR/NOT never act as
operators), every Lucene classic-syntax special character  \\ + - ! ( ) : ^ [ ] " { } ~ * ? | & /
is backslash-escaped, then each SQL single quote is doubled. The result is handed to TEXT_MATCH and
TEXT_SCORE unchanged: Pinot's classic QueryParser splits on whitespace, runs each chunk through the
index's analyzer (StandardAnalyzer + Pinot's 33 default English stop words) and ORs the terms
(default operator OR), i.e. a safe OR-of-terms query analysed exactly like the indexed text.
Queries that become empty after stripping are not sent (empty result).

Structured filter for hybrid_filter: brand is present (Pinot null handling on, `brand IS NOT NULL`).
It is query-independent (does not use the judgments), keeps ~94% of products (72,335 of
1,215,854 US products have a null brand) and only removes products with no brand, so it is a
neutral pre-filter whose purpose is to exercise filter push-down inside both retrievers.

Latency: each request's client wall time and the broker's timeUsedMs are recorded; the JSON
reports p50/p95/p99 over the queries after the first WARMUP (default 200) requests, i.e. warm.
Outputs: results/pinot_<table>_<mode>.json (+ .per_query.parquet) and data/runs/<same>.parquet.
"""
import argparse
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from common import QUERY_EMB, RESULTS_DIR
import evallib
from evallib import fuse_runs, load_run, run_retriever, save_run, write_results

_load_qrels_all = evallib.load_qrels


def sample_qids(version, n, seed):
    """Deterministic query sample: sorted query ids of the split, random.Random(seed).sample(n)."""
    import random
    queries, _ = _load_qrels_all(version=version)
    return set(random.Random(seed).sample(sorted(queries), n))


def load_qrels(version="large", sample=0, seed=2026):
    queries, qrels = _load_qrels_all(version=version)
    if sample:
        keep = sample_qids(version, sample, seed)
        queries = {q: t for q, t in queries.items() if q in keep}
        qrels = {q: j for q, j in qrels.items() if q in keep}
    return queries, qrels

TABLES = {"et": "esci_et", "local": "esci_local"}
RRF_K = 60
DEPTH = 100
SEG_K = 100          # VECTOR_SIMILARITY top-k per segment (--seg-k)
FILTER_SQL = "brand IS NOT NULL"
FILTER_DESC = "brand IS NOT NULL (enableNullHandling=true); keeps products that have a brand"

_LUCENE_SPECIAL = re.compile(r'([\\+\-!():^\[\]"{}~*?|&/])')


def lucene_query(q):
    """Raw ESCI query -> safe Lucene classic-syntax OR-of-terms query string (not SQL-quoted)."""
    q = _LUCENE_SPECIAL.sub(r"\\\1", q.lower())
    return " ".join(q.split())


def sql_str(s):
    return s.replace("'", "''")


def vec_literal(v):
    return "ARRAY[" + ",".join(format(float(x), ".8g") for x in v) + "]"


def bm25_sql(table, q, flt=None):
    lq = sql_str(lucene_query(q))
    where = f"TEXT_MATCH(text, '{lq}')" + (f" AND {flt}" if flt else "")
    return (f"SELECT product_id, TEXT_SCORE(text, '{lq}') AS s FROM {table} WHERE {where} "
            f"ORDER BY s DESC LIMIT {DEPTH}")


def vector_sql(table, v, flt=None):
    arr = vec_literal(v)
    where = f"VECTOR_SIMILARITY(embedding, {arr}, {SEG_K})" + (f" AND {flt}" if flt else "")
    return (f"SELECT product_id, cosineDistance(embedding, {arr}) AS d FROM {table} WHERE {where} "
            f"ORDER BY d LIMIT {DEPTH}")


def hybrid_sql(table, q, v, flt=None):
    lq = sql_str(lucene_query(q))
    arr = vec_literal(v)
    tf = f" AND {flt}" if flt else ""
    return (
        "WITH text_hits AS ("
        " SELECT product_id, ROW_NUMBER() OVER (ORDER BY s DESC, product_id) AS r FROM ("
        f"  SELECT product_id, TEXT_SCORE(text, '{lq}') AS s FROM {table}"
        f"  WHERE TEXT_MATCH(text, '{lq}'){tf}"
        f"  ORDER BY s DESC LIMIT {DEPTH})),"
        " vec_hits AS ("
        " SELECT product_id, ROW_NUMBER() OVER (ORDER BY d, product_id) AS r FROM ("
        f"  SELECT product_id, cosineDistance(embedding, {arr}) AS d FROM {table}"
        f"  WHERE VECTOR_SIMILARITY(embedding, {arr}, {SEG_K}){tf}"
        f"  ORDER BY d LIMIT {DEPTH}))"
        f" SELECT product_id, SUM(1.0 / ({RRF_K} + r)) AS rrf FROM ("
        " SELECT product_id, r FROM text_hits UNION ALL SELECT product_id, r FROM vec_hits)"
        f" GROUP BY product_id ORDER BY rrf DESC, product_id LIMIT {DEPTH}")


def hybrid_sql_vector_only(table, v, flt=None):
    """Hybrid when the text query is empty after escaping: only the vector list contributes."""
    arr = vec_literal(v)
    tf = f" AND {flt}" if flt else ""
    return (
        f"SELECT product_id, 1.0 / ({RRF_K} + ROW_NUMBER() OVER (ORDER BY d, product_id)) AS rrf FROM ("
        f" SELECT product_id, cosineDistance(embedding, {arr}) AS d FROM {table}"
        f" WHERE VECTOR_SIMILARITY(embedding, {arr}, {SEG_K}){tf}"
        f" ORDER BY d LIMIT {DEPTH}) ORDER BY rrf DESC, product_id LIMIT {DEPTH}")


def load_query_vectors():
    t = pq.read_table(QUERY_EMB)
    col = t.column("embedding").combine_chunks()
    vals = col.values.to_numpy(zero_copy_only=False).reshape(-1, 768).astype(np.float32)
    return dict(zip(t.column("query_id").to_pylist(), vals))


def pct(xs, p):
    return round(float(np.percentile(np.asarray(xs, dtype=float), p)), 1) if xs else None


class Client:
    def __init__(self, broker, options, timeout=120):
        self.url = broker.rstrip("/") + "/query/sql"
        self.options = options
        self.timeout = timeout
        self.local = threading.local()
        self.lock = threading.Lock()
        self.lat = []          # (seq, client_ms, broker_ms)
        self.errors = []
        self.partial = 0
        self.seq = 0

    def session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            s = self.local.s = requests.Session()
        return s

    def query(self, qid, sql):
        body = {"sql": sql}
        if self.options:
            body["queryOptions"] = self.options
        t0 = time.perf_counter()
        for attempt in range(3):
            try:
                r = self.session().post(self.url, json=body, timeout=self.timeout)
                d = r.json()
                break
            except Exception as e:  # noqa: BLE001 - network errors are retried then recorded
                if attempt == 2:
                    with self.lock:
                        self.errors.append({"qid": qid, "error": repr(e)[:300]})
                    return []
                time.sleep(1)
        ms = (time.perf_counter() - t0) * 1000
        exc = d.get("exceptions") or []
        with self.lock:
            self.seq += 1
            self.lat.append((self.seq, ms, d.get("timeUsedMs")))
            if exc:
                self.errors.append({"qid": qid, "error": json.dumps(exc)[:300]})
            if d.get("partialResult"):
                self.partial += 1
        if exc:
            return []
        rows = (d.get("resultTable") or {}).get("rows") or []
        return [row[0] for row in rows]

    def latency_summary(self, warmup):
        warm = [x for x in self.lat if x[0] > warmup] or self.lat
        cm = [x[1] for x in warm]
        bm = [x[2] for x in warm if x[2] is not None]
        return {"warmup_excluded": min(warmup, len(self.lat)), "n": len(warm),
                "client_ms_p50": pct(cm, 50), "client_ms_p95": pct(cm, 95), "client_ms_p99": pct(cm, 99),
                "broker_ms_p50": pct(bm, 50), "broker_ms_p95": pct(bm, 95), "broker_ms_p99": pct(bm, 99),
                "client_ms_mean": round(float(np.mean(cm)), 1) if cm else None}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", choices=list(TABLES), required=True)
    ap.add_argument("--mode", required=True,
                    choices=["bm25", "vector", "hybrid_sql", "hybrid_client", "hybrid_filter"])
    ap.add_argument("--version", default="large", choices=["large", "small"])
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--ef", type=int, default=0, help="vectorEfSearch (a node-visit cap in this build); 0 = unset")
    ap.add_argument("--seg-k", type=int, default=100, help="VECTOR_SIMILARITY per-segment top-k")
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--limit", type=int, default=0, help="smoke test: first N queries, no files")
    ap.add_argument("--sample", type=int, default=0, help="random sample of N queries of the split")
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--broker", default="http://localhost:18000")
    a = ap.parse_args()
    global SEG_K
    SEG_K = a.seg_k
    table = TABLES[a.table]
    suffix = ("" if a.version == "large" else "_small") + (f"_s{a.sample}" if a.sample else "")
    # run_retriever loads qrels through evallib.load_qrels: restrict it to the sample
    evallib.load_qrels = lambda path=None, version="large": load_qrels(version, a.sample, a.seed)
    name = f"pinot_{a.table}_{a.mode}{suffix}"

    if a.mode == "hybrid_client":
        _, qrels = load_qrels(a.version, a.sample, a.seed)
        def first_run(mode):  # the exact run for this query set, else a superset run (fusion is per query)
            for n in (f"pinot_{a.table}_{mode}{suffix}", f"pinot_{a.table}_{mode}_small", f"pinot_{a.table}_{mode}"):
                if (evallib.RUNS_DIR / f"{n}.parquet").exists():
                    return n
            raise SystemExit(f"no saved {mode} run for table {a.table}")
        b, v = first_run("bm25"), first_run("vector")
        fused = fuse_runs([load_run(b), load_run(v)], k=RRF_K, depth=DEPTH)
        fused = {q: fused.get(q, []) for q in qrels}
        save_run(name, fused)
        write_results(name, fused, qrels,
                      {"engine": "client-side RRF", "rrf_k": RRF_K, "inputs": [b, v],
                       "input_depth": DEPTH, "output_depth": DEPTH, "table": table},
                      {}, a.version)
        return

    opts = []
    if a.mode != "bm25" and a.ef > 0:
        opts.append(f"vectorEfSearch={a.ef}")
    if a.mode in ("hybrid_sql", "hybrid_filter"):
        opts.append("useMultistageEngine=true")
    if a.mode == "hybrid_filter":
        opts.append("enableNullHandling=true")
    opts.append("timeoutMs=120000")
    client = Client(a.broker, ";".join(opts))
    qvec = load_query_vectors() if a.mode != "bm25" else None
    flt = FILTER_SQL if a.mode == "hybrid_filter" else None

    def one(item):
        qid, q = item
        lq = lucene_query(q)
        if a.mode == "bm25":
            return client.query(qid, bm25_sql(table, q)) if lq.strip("\\ ") else []
        v = qvec[qid]
        if a.mode == "vector":
            return client.query(qid, vector_sql(table, v))
        if not lq.strip("\\ "):
            return client.query(qid, hybrid_sql_vector_only(table, v, flt))
        return client.query(qid, hybrid_sql(table, q, v, flt))

    pool = ThreadPoolExecutor(a.concurrency)

    def retriever(chunk):
        return list(pool.map(one, chunk))

    if a.limit:
        queries, _ = load_qrels(a.version, a.sample, a.seed)
        items = list(queries.items())[:a.limit]
        t0 = time.time()
        res = retriever(items)
        print(f"{len(items)} queries in {time.time() - t0:.1f}s; empty={sum(1 for r in res if not r)}; "
              f"errors={client.errors[:3]}; lat={client.latency_summary(0)}")
        for (qid, q), r in list(zip(items, res))[:3]:
            print(qid, q, r[:5])
        return

    cfg = {"engine": "StarTree Pinot 2.0.0-SNAPSHOT + OSS 1.7.0-SNAPSHOT (TEXT_SCORE)", "table": table,
           "mode": a.mode, "k": DEPTH, "concurrency": a.concurrency, "queryOptions": client.options,
           "bm25": "Lucene BM25Similarity k1=1.2 b=0.75, per-segment statistics",
           "text_analyzer": "Pinot default: Lucene StandardAnalyzer + 33 default English stop words, no stemming",
           "lucene_query": "lower-case, backslash-escape classic-syntax specials, OR of whitespace terms"}
    if a.mode != "bm25":
        cfg.update({"vector_index": "HNSW (Lucene, M=16, beamWidth=100 defaults), COSINE, dim 768",
                    "vectorEfSearch": a.ef or "unset (Lucene beam = per-segment k)", "per_segment_topk": SEG_K})
    if a.mode.startswith("hybrid"):
        cfg.update({"fusion": "RRF in multi-stage SQL (UNION ALL + GROUP BY)", "rrf_k": RRF_K})
    if flt:
        cfg["filter"] = FILTER_DESC
    if a.sample:
        cfg["query_sample"] = {"split": a.version, "n": a.sample, "seed": a.seed,
                               "how": "random.Random(seed).sample(sorted(query_ids), n)"}
    run_retriever(name, retriever, cfg, k=DEPTH, batch=512, version=a.version)
    pool.shutdown()
    out = RESULTS_DIR / f"{name}.json"
    d = json.loads(out.read_text())
    d["latency"] = client.latency_summary(a.warmup)
    d["errors"] = {"count": len(client.errors), "partial_results": client.partial,
                   "sample": client.errors[:5]}
    out.write_text(json.dumps(d, indent=2))
    print(json.dumps({"latency": d["latency"], "errors": d["errors"]["count"]}), flush=True)


if __name__ == "__main__":
    main()
