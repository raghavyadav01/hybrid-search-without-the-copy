"""Brand-scoped search on EC2: the same queries with `brand = <b>` on Pinot (brand-sorted lake, file pruning) and on
Elasticsearch / OpenSearch (filtered kNN / filtered hybrid over one graph).
Brand per query = most common brand among its Exact-judged products. Latency: client ms, 200 warm-up then N queries,
concurrency 1/8/32.   python3 brand_bench.py PINOT_HOST ES_HOST OS_HOST [N]"""
import json, sys, time, threading, collections
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np, pyarrow.fs as pafs, pyarrow.parquet as pq, requests
import pinot_runs as pr
import search_baselines as sb

P, ES, OS = sys.argv[1:4]; N = int(sys.argv[4]) if len(sys.argv) > 4 else 1000
TABLE = "esci_brand"; NC = 1000; DEPTH = 100
qs, qrels = pr.load_qrels("large", 0, 2026)
qtext = dict(qs) if isinstance(qs, list) else qs
qvec = pr.load_query_vectors()
fs = pafs.S3FileSystem(region="us-west-2")
files = [l.strip().replace("s3://", "") for l in open(Path.home() / "lake_files.txt")]
pid2brand = {}
for f in files:
    t = pq.read_table(f, filesystem=fs, columns=["product_id", "brand"], partitioning=None)
    pid2brand.update(zip(t.column("product_id").to_pylist(), t.column("brand").to_pylist()))
items = []
for qid, rel in qrels.items():
    bs = [pid2brand.get(p) for p, g in rel.items() if g in ("E", 1.0) and pid2brand.get(p) and "'" not in pid2brand.get(p)]
    if bs and qid in qvec and pr.lucene_query(qtext[qid]).strip("\\ "):
        items.append((qid, collections.Counter(bs).most_common(1)[0][0]))
rng = np.random.default_rng(2026); rng.shuffle(items)
warm, meas = items[:200], items[200:200 + N]
print(len(items), "queries with an Exact branded product; measuring", len(meas), flush=True)

S = requests.Session()
def pinot(mode, qid, b):
    flt = f"brand = '{pr.sql_str(b)}'"
    v = qvec[qid]
    if mode == "vector":
        sql, opts = pr.vector_sql(TABLE, v, flt), "timeoutMs=120000"
    else:
        st = requests.post(f"http://{P}:18000/query/sql", json={"sql": pr.stats_sql(TABLE, qtext[qid])}).json()
        stats = st["resultTable"]["rows"][0][0]
        sql, opts = pr.hybrid_sql(TABLE, qtext[qid], v, flt, stats), "useMultistageEngine=true;enableNullHandling=true;timeoutMs=120000"
    d = requests.post(f"http://{P}:18000/query/sql", json={"sql": sql, "queryOptions": opts}).json()
    if d.get("exceptions"): raise RuntimeError(str(d["exceptions"])[:300])
    return [r[0] for r in d["resultTable"]["rows"]]

def engine(e, host, mode, qid, b):
    vec = [float(x) for x in qvec[qid]]; filt = {"term": {"brand": b}}
    match = {"bool": {"must": {"match": {"text": {"query": qtext[qid]}}}, "filter": filt}}
    port = 9250 if e == "es" else 9260; params = None
    if e == "es":
        knn = {"field": "embedding", "query_vector": vec, "k": DEPTH, "num_candidates": NC, "filter": filt}
        body = ({"size": DEPTH, "_source": False, "knn": knn} if mode == "vector" else
                {"size": DEPTH, "_source": False, "retriever": {"rrf": {"retrievers": [{"standard": {"query": match}},
                 {"knn": knn}], "rank_window_size": DEPTH, "rank_constant": 60}}})
    else:
        knn = {"knn": {"embedding": {"vector": vec, "k": DEPTH, "filter": filt, "method_parameters": {"ef_search": NC}}}}
        body = ({"size": DEPTH, "_source": False, "query": knn} if mode == "vector" else
                {"size": DEPTH, "_source": False, "query": {"hybrid": {"queries": [match, knn], "pagination_depth": DEPTH}}})
        params = None if mode == "vector" else {"search_pipeline": sb.PIPELINE}
    r = requests.post(f"http://{host}:{port}/{sb.INDEX}/_search", json=body, params=params, timeout=60).json()
    if "hits" not in r: raise RuntimeError(str(r)[:300])
    return [h["_id"] for h in r["hits"]["hits"]]

def run(fn, conc):
    lat, errs = [], []
    def one(it):
        t = time.perf_counter()
        try: fn(*it)
        except Exception as ex: errs.append(str(ex)[:200])
        lat.append((time.perf_counter() - t) * 1000)
    with ThreadPoolExecutor(conc) as ex: list(ex.map(one, warm))
    lat.clear()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(conc) as ex: list(ex.map(one, meas))
    wall = time.perf_counter() - t0
    a = np.array(lat)
    return {"p50": round(float(np.percentile(a, 50)), 1), "p99": round(float(np.percentile(a, 99)), 1),
            "qps": round(len(meas) / wall, 1), "errors": len(errs), "err": errs[:1]}

out = {"queries": len(meas), "num_candidates_or_ef": NC}
# sanity: top-10 overlap Pinot vs ES on 50 queries
ov = [len(set(pinot("vector", q, b)[:10]) & set(engine("es", ES, "vector", q, b)[:10])) / 10 for q, b in meas[:50]]
out["vector_top10_overlap_pinot_es"] = round(float(np.mean(ov)), 3)
print("overlap", out["vector_top10_overlap_pinot_es"], flush=True)
for mode in ["vector", "hybrid"]:
    for name, fn in [("pinot", lambda q, b, m=mode: pinot(m, q, b)), ("es", lambda q, b, m=mode: engine("es", ES, m, q, b)),
                     ("os", lambda q, b, m=mode: engine("os", OS, m, q, b))]:
        for c in [1, 8, 32]:
            r = run(fn, c); out.setdefault(mode, {}).setdefault(name, {})[f"c{c}"] = r
            print(mode, name, c, r, flush=True)
Path("results/ec2_brand_bench.json").write_text(json.dumps(out, indent=2))
