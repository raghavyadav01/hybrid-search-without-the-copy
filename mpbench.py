"""Multi-process closed-loop load generator (one OS process per concurrent client, so the Python GIL does not cap
throughput). Same queries, sample and seed as pinot_runs / search_baselines latency passes.

  python3 mpbench.py --pinot HOST --es HOST --os HOST --cases CASES [--n 1000] [--warm 200] [--conc 1,8,32,64]
     --out results/FILE.json

CASES: comma list of system:mode[:table[:k]] — systems pinot|es|os; modes bm25|vector|hybrid|vector_brand|hybrid_brand.
  pinot:vector:esci_et:100   pinot:hybrid:esci_big:100   es:vector   os:hybrid_brand ...
ES/OS kNN use --nc candidates (num_candidates / ef_search). *_brand modes add `brand = <b>` (b = most common brand of
the query's Exact products) to both retrievers.
"""
import argparse
import collections
import json
import re
import multiprocessing as mp
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import requests

import pinot_runs as pr
from concurrent.futures import ThreadPoolExecutor
from evallib import rrf
import search_baselines as sb

DEPTH = 100
G = {}
# Corpus statistics are fixed for a snapshot, so they are cached per analyzed term (DFS-style) once seen. A query uses
# the cache only when it is plain lowercase ASCII words, whose terms the index's StandardAnalyzer produces unchanged;
# anything else asks the engine.
STOP = set("a an and are as at be but by for if in into is it no not of on or such that the their then there these "
           "they this to was will with".split())
STATS = {"coll": None, "terms": {}, "hits": 0, "misses": 0, "complete": False}


def query_terms(q):
    lq = q.lower()
    # Spaces and punctuation that the StandardAnalyzer always splits on; '.', ',', ':' and apostrophes can join tokens
    if not re.fullmatch(r"[a-z0-9 \-&/()+!?\"#%*]+", lq):
        return None
    return [t for t in re.findall(r"[a-z0-9]+", lq) if t not in STOP]


def remember_stats(q, stats_json):
    d = json.loads(stats_json)["fields"]["text"]
    STATS["coll"] = {k: d[k] for k in ("maxDoc", "docCount", "sumTotalTermFreq", "sumDocFreq")}
    terms = d.get("terms", {})
    STATS["terms"].update(terms)
    for t in query_terms(q) or []:
        STATS["terms"].setdefault(t, [0, 0])


def cached_stats(q):
    terms = query_terms(q)
    if not terms or STATS["coll"] is None:
        return None
    if not STATS["complete"] and any(t not in STATS["terms"] for t in terms):
        return None
    # With the whole vocabulary materialized, a term missing from it occurs in no document
    return json.dumps({"fields": {"text": dict(STATS["coll"], terms={t: STATS["terms"].get(t, [0, 0]) for t in terms})}},
                      separators=(",", ":"))


def get_stats(s, url, table, q):
    if G.get("cache_stats"):
        st = cached_stats(q)
        if st is not None:
            STATS["hits"] += 1
            return st
        STATS["misses"] += 1
    st = s.post(url, json={"sql": pr.stats_sql(table, q)}).json()["resultTable"]["rows"][0][0]
    if G.get("cache_stats"):
        remember_stats(q, st)
    return st


def load_items(n, warm, brand):
    qs, qrels = pr.load_qrels("large", 0, 2026)
    qtext = dict(qs) if isinstance(qs, list) else qs
    qvec = pr.load_query_vectors()
    items = []
    if brand:
        import pyarrow.fs as pafs
        import pyarrow.parquet as pq
        fs = pafs.S3FileSystem(region="us-west-2")
        pid2brand = {}
        for f in open(Path.home() / "lake_files.txt"):
            t = pq.read_table(f.strip().replace("s3://", ""), filesystem=fs, columns=["product_id", "brand"],
                              partitioning=None)
            pid2brand.update(zip(t.column("product_id").to_pylist(), t.column("brand").to_pylist()))
        for qid, rel in qrels.items():
            bs = [pid2brand.get(p) for p, g in rel.items() if g in ("E", 1.0) and pid2brand.get(p)
                  and "'" not in pid2brand.get(p)]
            if bs and qid in qvec and pr.lucene_query(qtext[qid]).strip("\\ "):
                items.append((qid, collections.Counter(bs).most_common(1)[0][0]))
    else:
        items = [(qid, None) for qid in qrels if qid in qvec and pr.lucene_query(qtext[qid]).strip("\\ ")]
    rng = np.random.default_rng(2026)
    rng.shuffle(items)
    G["prefill_items"] = items[warm + n:]
    return qtext, qvec, items[:warm], items[warm:warm + n]


def post_ids(s, url, sql, opts):
    d = s.post(url, json={"sql": sql, "queryOptions": opts}).json()
    if d.get("exceptions"):
        raise RuntimeError(str(d["exceptions"])[:200])
    return [r[0] for r in d["resultTable"]["rows"]]


def pinot_call(s, host, mode, table, k, qid, b):
    pr.SEG_K, pr.VECTOR_FN = k, "index"
    q, v = G["qtext"][qid], G["qvec"][qid]
    flt = f"brand = '{pr.sql_str(b)}'" if b else None
    url = f"http://{host}:18000/query/sql"
    if mode.startswith("hybridc"):
        # Hybrid as two single-stage requests sent in parallel and fused with RRF (k=60) by the application
        if "pool" not in G:
            G["pool"], G["s2"] = ThreadPoolExecutor(2), requests.Session()
        opts = "timeoutMs=120000"
        fb = G["pool"].submit(post_ids, s, url, pr.bm25_sql(table, q, flt, get_stats(s, url, table, q)), opts)
        fv = G["pool"].submit(post_ids, G["s2"], url, pr.vector_sql(table, v, flt), opts)
        return rrf([fb.result(), fv.result()], 60, DEPTH)
    if mode.startswith("bm25"):
        sql, opts = pr.bm25_sql(table, q, flt, get_stats(s, url, table, q)), "timeoutMs=120000"
    elif mode.startswith("vector"):
        sql, opts = pr.vector_sql(table, v, flt), "timeoutMs=120000"
    else:
        sql = pr.hybrid_sql(table, q, v, flt, get_stats(s, url, table, q))
        opts = "useMultistageEngine=true;timeoutMs=120000" + (";enableNullHandling=true" if b else "")
    d = s.post(url, json={"sql": sql, "queryOptions": opts}).json()
    if d.get("exceptions"):
        raise RuntimeError(str(d["exceptions"])[:200])
    return [r[0] for r in d["resultTable"]["rows"]]


def engine_call(s, e, host, mode, nc, qid, b):
    q, vec = G["qtext"][qid], [float(x) for x in G["qvec"][qid]]
    filt = {"term": {"brand": b}} if b else None
    match = {"match": {"text": {"query": q}}}
    if filt:
        match = {"bool": {"must": match, "filter": filt}}
    params = None
    if mode.startswith("bm25"):
        body = {"size": DEPTH, "_source": False, "query": match}
    elif e == "es":
        knn = {"field": "embedding", "query_vector": vec, "k": DEPTH, "num_candidates": nc}
        if filt:
            knn["filter"] = filt
        body = ({"size": DEPTH, "_source": False, "knn": knn} if mode.startswith("vector") else
                {"size": DEPTH, "_source": False, "retriever": {"rrf": {"retrievers": [
                    {"standard": {"query": match}}, {"knn": knn}], "rank_window_size": DEPTH, "rank_constant": 60}}})
    else:
        kq = {"vector": vec, "k": DEPTH, "method_parameters": {"ef_search": nc}}
        if filt:
            kq["filter"] = filt
        knn = {"knn": {"embedding": kq}}
        if mode.startswith("vector"):
            body = {"size": DEPTH, "_source": False, "query": knn}
        else:
            body = {"size": DEPTH, "_source": False,
                    "query": {"hybrid": {"queries": [match, knn], "pagination_depth": DEPTH}}}
            params = {"search_pipeline": sb.PIPELINE}
    port = 9250 if e == "es" else 9260
    r = s.post(f"http://{host}:{port}/{sb.INDEX}/_search", json=body, params=params, timeout=120).json()
    if "hits" not in r:
        raise RuntimeError(str(r)[:200])
    return [h["_id"] for h in r["hits"]["hits"]]


def worker(args):
    case, chunk, start_at, idx = args
    s = requests.Session()
    system, mode, table, k, host, nc = case
    hosts = host.split(",")  # several replicas: each client process is pinned to one broker, round-robin
    host = hosts[idx % len(hosts)]
    lat, errs, ids = [], 0, {}
    STATS["hits"] = STATS["misses"] = 0
    while time.time() < start_at:
        time.sleep(0.001)
    for qid, b in chunk:
        t = time.perf_counter()
        try:
            res = (pinot_call(s, host, mode, table, k, qid, b) if system == "pinot"
                   else engine_call(s, system, host, mode, nc, qid, b))
            ids[qid] = res[:DEPTH]
        except Exception:  # noqa: BLE001
            errs += 1
        lat.append((time.perf_counter() - t) * 1000)
    return lat, errs, time.time(), ids, STATS["hits"], STATS["misses"]


def run(pool, case, items, conc):
    chunks = [items[i::conc] for i in range(conc)]
    start_at = time.time() + 0.5
    res = pool.map(worker, [(case, c, start_at, i) for i, c in enumerate(chunks)])
    lat = np.concatenate([np.array(r[0]) for r in res])
    wall = max(r[2] for r in res) - start_at
    ids = {}
    for r in res:
        ids.update(r[3])
    hits, misses = sum(r[4] for r in res), sum(r[5] for r in res)
    return {"stats_cache_hit": round(hits / (hits + misses), 3) if hits + misses else None,
            "n": len(lat), "p50": round(float(np.percentile(lat, 50)), 1),
            "p95": round(float(np.percentile(lat, 95)), 1), "p99": round(float(np.percentile(lat, 99)), 1),
            "qps": round(len(lat) / wall, 1), "errors": int(sum(r[1] for r in res))}, ids


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pinot")
    ap.add_argument("--es")
    ap.add_argument("--os")
    ap.add_argument("--cases", required=True)
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--warm", type=int, default=200)
    ap.add_argument("--conc", default="1,8,32,64")
    ap.add_argument("--nc", type=int, default=1000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--term-stats", help="corpus statistics materialized for the whole vocabulary (JSON from "
                    "ec2/build_term_stats.py); terms missing from it fall back to a live statistics query")
    ap.add_argument("--cache-stats", type=int, default=0,
                    help="cache corpus statistics per term, prefilled from this many other ESCI queries (0 = off)")
    a = ap.parse_args()
    cases = [c.split(":") for c in a.cases.split(",")]
    brand = any("brand" in c[1] for c in cases)
    G["qtext"], G["qvec"], warm, meas = load_items(a.n, a.warm, brand)
    out = json.loads(Path(a.out).read_text()) if Path(a.out).exists() else {}
    if a.term_stats:
        G["cache_stats"] = True
        ts = json.loads(Path(a.term_stats).read_text())
        STATS["coll"], STATS["terms"], STATS["complete"] = ts["coll"], ts["terms"], True
        print("term statistics loaded:", len(STATS["terms"]), "terms", flush=True)
    if a.cache_stats:
        from concurrent.futures import ThreadPoolExecutor
        G["cache_stats"] = True
        table = next((c[2] for c in cases if c[0] == "pinot" and len(c) > 2), "esci_et")
        url = f"http://{a.pinot.split(',')[0]}:18000/query/sql"
        pre = [G["qtext"][qid] for qid, _ in G["prefill_items"][:a.cache_stats]]
        sess = requests.Session()
        with ThreadPoolExecutor(16) as ex:
            got = list(ex.map(lambda q: (q, sess.post(url, json={"sql": pr.stats_sql(table, q)}).json()), pre))
        for q, d in got:
            if not d.get("exceptions"):
                remember_stats(q, d["resultTable"]["rows"][0][0])
        print("stats cache prefilled from", len(pre), "queries:", len(STATS["terms"]), "terms", flush=True)
    host = {"pinot": a.pinot, "es": a.es, "os": a.os}
    ctx = mp.get_context("fork")
    concs = [int(x) for x in a.conc.split(",")]
    with ctx.Pool(max(concs)) as pool:
        for c in cases:
            system, mode = c[0], c[1]
            table = c[2] if len(c) > 2 else "esci_et"
            k = int(c[3]) if len(c) > 3 else 100
            case = (system, mode, table, k, host[system], a.nc)
            key = ":".join(c) if system == "pinot" else f"{system}:{mode}:nc{a.nc}"
            run(pool, case, warm, min(8, max(concs)))  # warm-up
            for conc in concs:
                r, ids = run(pool, case, meas, conc)
                out.setdefault(key, {})[f"c{conc}"] = r
                print(key, conc, r, flush=True)
            Path(a.out).write_text(json.dumps(out, indent=2))
            Path(a.out.replace(".json", f".{key.replace(':', '_')}.ids.json")).write_text(json.dumps(ids))


if __name__ == "__main__":
    main()
