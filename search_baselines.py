"""Elasticsearch / OpenSearch baselines for the ESCI (US) evaluation.

  .venv/bin/python -I search_baselines.py --engine es|os up   [--heap 4g] [--mem 9g]
  .venv/bin/python -I search_baselines.py --engine es|os index [--limit-docs N] [--index NAME]
  .venv/bin/python -I search_baselines.py --engine es|os run --mode MODE [--concurrency 8]
                                          [--limit N] [--index NAME]
  .venv/bin/python -I search_baselines.py --engine es|os down      (stops the container, keeps the volume)
  .venv/bin/python -I search_baselines.py --engine es|os timings   (writes results/<es|os>_timings.json)
  .venv/bin/python -I search_baselines.py --engine es report       (writes results/baselines.md)

Containers (single node, security off, one at a time):
  es: elasticsearch:9.5.5  container esci-es  volume esci-es-data  http://localhost:9250
  os: opensearchproject/opensearch:3.9.0  container esci-os  volume esci-os-data  http://localhost:9260

Index `esci_products` (1 shard, 0 replicas):
  _id = product_id; product_id keyword; brand, color keyword; text = `text` column of
  products_us.parquet (common.product_text recipe) analysed with the `standard` analyzer plus
  Lucene's 33 English stop words (`_english_`), no stemming (matches ref_bm25 / Pinot's default);
  embedding = 768-d float, dot_product on L2-normalised bge-base vectors (= cosine), HNSW m=16,
  ef_construction=100, no quantisation (ES: dense_vector index_options type=hnsw; OS: knn_vector,
  method hnsw, engine lucene, space_type innerproduct). _source is disabled to save disk; hits
  return the _id only. BM25 = Lucene BM25Similarity defaults (k1=1.2, b=0.75).

Modes (top-100 each; per-request client wall time and the engine's `took` are recorded; p50/p95/p99
are over requests after the first WARMUP=500, i.e. warm):
  bm25          match query on text (OR of analysed terms)
  knn           k=100, num_candidates=200 (ES) / ef_search=200 (OS lucene method_parameters)
  hybrid_native RRF k=60, window 100 inside the engine. ES: retriever.rrf over a standard(match)
                and a knn retriever (needs the Enterprise/trial license: started via
                POST /_license/start_trial). OS: hybrid query + search pipeline with
                score-ranker-processor, combination technique rrf, rank_constant 60.
  hybrid_client client-side evallib.fuse_runs (k=60) over this engine's saved bm25 and knn runs;
                latency = sum of the two requests' client times per query (sequential client).
Outputs: results/<es|os>_<mode>.json (+ .per_query.parquet with latency_ms) and
data/runs/<same>.parquet.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from common import QUERY_EMB, RESULTS_DIR
from evallib import fuse_runs, load_qrels, load_run, run_retriever, save_run, write_results

ENGINES = {
    "es": {"image": "elasticsearch:9.5.5", "container": "esci-es", "volume": "esci-es-data",
           "port": 9250, "data": "/usr/share/elasticsearch/data",
           "env": {"discovery.type": "single-node", "xpack.security.enabled": "false",
                   "xpack.ml.enabled": "false"}, "heap_env": "ES_JAVA_OPTS"},
    "os": {"image": "opensearchproject/opensearch:3.9.0", "container": "esci-os",
           "volume": "esci-os-data", "port": 9260, "data": "/usr/share/opensearch/data",
           "env": {"discovery.type": "single-node", "DISABLE_SECURITY_PLUGIN": "true",
                   "DISABLE_INSTALL_DEMO_CONFIG": "true"}, "heap_env": "OPENSEARCH_JAVA_OPTS"},
}
INDEX = "esci_products"
RRF_K, DEPTH, NUM_CAND = 60, 100, 200
SUFFIX = ""   # result-name suffix for non-default num_candidates (e.g. "_c1000")
PIPELINE = "esci-rrf"
BULK_DOCS = 500


def base(engine):
    # ESCI_SEARCH_HOST: run the client on another box (EC2 load generator)
    return f"http://{os.environ.get('ESCI_SEARCH_HOST', 'localhost')}:{ENGINES[engine]['port']}"


def pct(xs, p):
    return round(float(np.percentile(np.asarray(xs, dtype=float), p)), 1) if xs else None


# ---------------------------------------------------------------- container lifecycle
def up(engine, heap, mem):
    e = ENGINES[engine]
    running = subprocess.run(["docker", "ps", "-aq", "-f", f"name=^{e['container']}$"],
                             capture_output=True, text=True).stdout.strip()
    if running:
        subprocess.run(["docker", "start", e["container"]], check=True)
    else:
        cmd = ["docker", "run", "-d", "--name", e["container"], "-m", mem,
               "-p", f"{os.environ.get('ESCI_BIND', '127.0.0.1')}:{e['port']}:9200", "-v", f"{e['volume']}:{e['data']}",
               "--ulimit", "memlock=-1:-1", "--ulimit", "nofile=65536:65536",
               "-e", f"{e['heap_env']}=-Xms{heap} -Xmx{heap}"]
        for k, v in e["env"].items():
            cmd += ["-e", f"{k}={v}"]
        subprocess.run(cmd + [e["image"]], check=True)
    for _ in range(180):
        try:
            r = requests.get(base(engine) + "/_cluster/health", timeout=2)
            if r.ok and r.json().get("status") in ("green", "yellow"):
                print(json.dumps(requests.get(base(engine)).json()["version"]))
                insp = json.loads(subprocess.run(["docker", "inspect", e["container"]], capture_output=True,
                                                 text=True).stdout)[0]
                info = {"container": e["container"], "created": insp["Created"],
                        "mem_limit_bytes": insp["HostConfig"]["Memory"],
                        "heap_env": [x for x in insp["Config"]["Env"] if x.startswith(e["heap_env"])],
                        "reused_existing_container": bool(running),
                        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
                (RESULTS_DIR / f"{engine}_container.json").write_text(json.dumps(info, indent=2))
                print(json.dumps(info))
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise SystemExit("engine did not come up")


def down(engine):
    subprocess.run(["docker", "stop", ENGINES[engine]["container"]], check=True)


# ---------------------------------------------------------------- indexing
def index_body(engine):
    settings = {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1",
                "analysis": {"analyzer": {"std_english_stop": {"type": "standard",
                                                               "stopwords": "_english_"}}}}
    props = {"product_id": {"type": "keyword"},
             "brand": {"type": "keyword", "ignore_above": 512},
             "color": {"type": "keyword", "ignore_above": 512},
             "text": {"type": "text", "analyzer": "std_english_stop"}}
    if engine == "es":
        props["embedding"] = {"type": "dense_vector", "dims": 768, "index": True,
                              "similarity": "dot_product",
                              "index_options": {"type": "hnsw", "m": 16, "ef_construction": 100}}
    else:
        settings["index.knn"] = True
        props["embedding"] = {"type": "knn_vector", "dimension": 768,
                              "method": {"name": "hnsw", "engine": "lucene",
                                         "space_type": "innerproduct",
                                         "parameters": {"m": 16, "ef_construction": 100}}}
    return {"settings": settings, "mappings": {"_source": {"enabled": False}, "properties": props}}


def _post_bulk(args):
    url, index, ids, texts, brands, colors, emb, prep_only = args
    tp = time.perf_counter()
    lines = []
    for pid, t, b, c, v in zip(ids, texts, brands, colors, emb):
        lines.append('{"index":{"_index":"%s","_id":%s}}' % (index, json.dumps(pid)))
        doc = {"product_id": pid, "text": t or "", "embedding": v.tolist()}
        if b:
            doc["brand"] = b
        if c:
            doc["color"] = c
        lines.append(json.dumps(doc, separators=(",", ":")))
    body = ("\n".join(lines) + "\n").encode()
    prep_s = time.perf_counter() - tp
    if prep_only:
        return len(ids), [], 0, prep_s, 0.0
    tq = time.perf_counter()
    for attempt in range(5):
        try:
            r = requests.post(url + "/_bulk", data=body, timeout=600,
                              headers={"Content-Type": "application/x-ndjson"})
            d = r.json()
            if r.status_code == 429 or (d.get("errors") and any(
                    it["index"].get("status") == 429 for it in d["items"])):
                time.sleep(5 * (attempt + 1))
                continue
            errs = [it["index"]["error"] for it in d["items"] if "error" in it["index"]]
            return len(ids), errs[:2], len(errs), prep_s, time.perf_counter() - tq
        except requests.RequestException as ex:  # noqa: PERF203
            if attempt == 4:
                return len(ids), [repr(ex)[:300]], len(ids), prep_s, time.perf_counter() - tq
            time.sleep(5)
    return len(ids), ["429 after retries"], len(ids), prep_s, time.perf_counter() - tq


def wait_merges(url, index, quiet_secs=30, max_secs=7200):
    t0, quiet_since = time.time(), None
    while time.time() - t0 < max_secs:
        st = requests.get(f"{url}/{index}/_stats/merge", timeout=30).json()
        cur = st["_all"]["primaries"]["merges"]["current"]
        if cur == 0:
            quiet_since = quiet_since or time.time()
            if time.time() - quiet_since >= quiet_secs:
                return time.time() - t0 - quiet_secs
        else:
            quiet_since = None
        time.sleep(5)
    return time.time() - t0


LAKE_TABLE = "esci.products_us_pi"


def lake_files():
    """Data files of the Iceberg table every system reads (same rows as Pinot ET / local)."""
    import pyarrow.fs as pafs
    if os.environ.get("ESCI_LAKE_FILES"):  # EC2: the same files in real S3 (instance-role credentials)
        files = sorted(x.strip() for x in open(os.environ["ESCI_LAKE_FILES"]) if x.strip())
        return pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-west-2")), \
            [f.replace("s3://", "") for f in files]
    from build_table import catalog
    tbl = catalog().load_table(LAKE_TABLE)
    files = sorted(tbl.inspect.files().to_pandas().file_path)
    fs = pafs.S3FileSystem(endpoint_override="http://localhost:9020", access_key="minioadmin",
                           secret_key="minioadmin", region="us-east-1", scheme="http")
    return fs, [f.replace("s3://", "") for f in files]


def index(engine, name, limit_docs, workers, prep_only=False):
    url = base(engine)
    if not prep_only:
        requests.delete(f"{url}/{name}", timeout=60)
        r = requests.put(f"{url}/{name}", json=index_body(engine), timeout=60)
        r.raise_for_status()
    t_load = time.time()
    fs, files = lake_files()
    load_secs = time.time() - t_load
    sent, nerr, sample = 0, 0, []
    prep_sum, post_sum, read_sum = 0.0, 0.0, 0.0
    t0 = time.time()
    with ProcessPoolExecutor(workers) as ex:
        futs = []
        for s in files:
            tr = time.time()
            t = pq.read_table(s, filesystem=fs, columns=["product_id", "text", "brand", "color", "embedding"], partitioning=None)
            ids = t.column("product_id").to_pylist()
            texts, brands, colors = (t.column(c).to_pylist() for c in ("text", "brand", "color"))
            emb = t.column("embedding").combine_chunks().flatten().to_numpy(
                zero_copy_only=False).reshape(-1, 768).astype(np.float32)
            del t
            read_sum += time.time() - tr
            for i in range(0, len(ids), BULK_DOCS):
                if limit_docs and sent >= limit_docs:
                    break
                cid = ids[i:i + BULK_DOCS]
                if limit_docs:
                    cid = cid[:limit_docs - sent]
                futs.append(ex.submit(_post_bulk, (
                    url, name, cid, texts[i:i + len(cid)], brands[i:i + len(cid)],
                    colors[i:i + len(cid)], emb[i:i + len(cid)], prep_only)))
                sent += len(cid)
                while len(futs) > workers * 3:
                    n, errs, ne, ps, qs = futs.pop(0).result()
                    nerr += ne
                    prep_sum += ps
                    post_sum += qs
                    sample += errs[:1] if len(sample) < 5 else []
            print(f"{s.rsplit('/', 1)[-1]}: queued {sent} docs, {time.time() - t0:.0f}s", flush=True)
            if limit_docs and sent >= limit_docs:
                break
        for f in futs:
            n, errs, ne, ps, qs = f.result()
            nerr += ne
            prep_sum += ps
            post_sum += qs
            sample += errs[:1] if len(sample) < 5 else []
    bulk_secs = time.time() - t0
    if prep_only:
        out = {"engine": engine, "prep_only": True, "docs": sent, "workers": workers,
               "wall_secs_incl_parquet_read": round(bulk_secs, 1),
               "doc_prep_cpu_secs_sum": round(prep_sum, 1), "input_load_secs": round(load_secs, 1)}
        print(json.dumps(out, indent=2))
        (RESULTS_DIR / f"{engine}_prep_only.json").write_text(json.dumps(out, indent=2))
        return
    t1 = time.time()
    requests.post(f"{url}/{name}/_refresh", timeout=3600).raise_for_status()
    refresh_secs = time.time() - t1
    requests.put(f"{url}/{name}/_settings", json={"index": {"refresh_interval": "1s"}}, timeout=60)
    merge_secs = wait_merges(url, name)
    seg_before = requests.get(f"{url}/{name}/_stats/segments", timeout=60).json()["_all"]["primaries"]["segments"]["count"]
    t2 = time.time()
    fm = requests.post(f"{url}/{name}/_forcemerge", params={"max_num_segments": 1}, timeout=14400)
    fm_status = fm.status_code
    wait_merges(url, name, quiet_secs=10)   # forcemerge can return before the merge thread ends
    force_secs = time.time() - t2
    requests.post(f"{url}/{name}/_flush", timeout=3600)
    st = requests.get(f"{url}/{name}/_stats/docs,store,segments", timeout=60).json()["_all"]["primaries"]
    cont = ENGINES[engine]["container"]
    du = subprocess.run(["docker", "exec", cont, "du", "-sb", ENGINES[engine]["data"]],
                        capture_output=True, text=True).stdout.split()
    out = {"engine": engine, "index": name, "version": requests.get(url).json()["version"],
           "docs_sent": sent, "docs_indexed": st["docs"]["count"], "bulk_errors": nerr,
           "error_sample": sample, "bulk_workers": workers, "bulk_docs": BULK_DOCS,
           "bulk_secs": round(bulk_secs, 1), "input_load_secs": round(load_secs, 1),
           "input_parquet_read_secs_sum": round(read_sum, 1), "input": LAKE_TABLE + " (Iceberg data files on MinIO)",
           "doc_prep_secs_sum": round(prep_sum, 1), "bulk_post_secs_sum": round(post_sum, 1),
           "refresh_secs": round(refresh_secs, 1),
           "merge_settle_secs": round(merge_secs, 1),
           "index_secs_total": round(bulk_secs + refresh_secs + merge_secs, 1),
           "segments_before_force_merge": seg_before, "force_merge_secs": round(force_secs, 1),
           "force_merge_http_status": fm_status,
           "index_secs_total_incl_force_merge": round(bulk_secs + refresh_secs + merge_secs + force_secs, 1),
           "store_bytes": st["store"]["size_in_bytes"], "segments": st["segments"]["count"],
           "data_dir_bytes": int(du[0]) if du else None,
           "mapping": index_body(engine), "written_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    print(json.dumps({k: v for k, v in out.items() if k != "mapping"}, indent=2))
    if not limit_docs:
        (RESULTS_DIR / f"{engine}_index.json").write_text(json.dumps(out, indent=2))


# ---------------------------------------------------------------- queries
def load_query_vectors():
    t = pq.read_table(QUERY_EMB)
    vals = t.column("embedding").combine_chunks().values.to_numpy(
        zero_copy_only=False).reshape(-1, 768).astype(np.float32)
    return dict(zip(t.column("query_id").to_pylist(), vals))


def query_body(engine, mode, q, v):
    match = {"match": {"text": {"query": q}}}
    if mode == "bm25":
        return {"size": DEPTH, "_source": False, "query": match}, None
    vec = [float(x) for x in v]
    if engine == "es":
        knn = {"field": "embedding", "query_vector": vec, "k": DEPTH, "num_candidates": NUM_CAND}
        if mode == "knn":
            return {"size": DEPTH, "_source": False, "knn": knn}, None
        return {"size": DEPTH, "_source": False, "retriever": {"rrf": {
            "retrievers": [{"standard": {"query": match}}, {"knn": knn}],
            "rank_window_size": DEPTH, "rank_constant": RRF_K}}}, None
    knn = {"knn": {"embedding": {"vector": vec, "k": DEPTH,
                                 "method_parameters": {"ef_search": NUM_CAND}}}}
    if mode == "knn":
        return {"size": DEPTH, "_source": False, "query": knn}, None
    return ({"size": DEPTH, "_source": False,
             "query": {"hybrid": {"queries": [match, knn], "pagination_depth": DEPTH}}},
            {"search_pipeline": PIPELINE})


def setup_native(engine):
    url = base(engine)
    if engine == "es":
        lic = requests.get(url + "/_license", timeout=30).json()["license"]
        if lic["type"] not in ("trial", "enterprise", "platinum"):
            r = requests.post(url + "/_license/start_trial?acknowledge=true", timeout=30)
            print("start_trial:", r.text)
        return requests.get(url + "/_license", timeout=30).json()["license"]["type"]
    r = requests.put(f"{url}/_search/pipeline/{PIPELINE}", timeout=30, json={
        "description": "RRF k=60 for hybrid queries",
        "phase_results_processors": [{"score-ranker-processor": {
            "combination": {"technique": "rrf", "rank_constant": RRF_K}}}]})
    r.raise_for_status()
    return "score-ranker-processor rrf"


class Client:
    def __init__(self, url, name):
        self.url = f"{url}/{name}/_search"
        self.local = threading.local()
        self.lock = threading.Lock()
        self.lat, self.errors, self.seq, self.by_qid = [], [], 0, {}

    def session(self):
        s = getattr(self.local, "s", None)
        if s is None:
            s = self.local.s = requests.Session()
        return s

    def query(self, qid, body, params):
        data = json.dumps(body)
        t0 = time.perf_counter()
        d = None
        for attempt in range(3):
            try:
                r = self.session().post(self.url, data=data, params={"request_cache": "false", **(params or {})},
                                        timeout=120,
                                        headers={"Content-Type": "application/json"})
                d = r.json()
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    with self.lock:
                        self.errors.append({"qid": qid, "error": repr(e)[:300]})
                    return []
                time.sleep(1)
        ms = (time.perf_counter() - t0) * 1000
        with self.lock:
            self.seq += 1
            self.lat.append((self.seq, ms, d.get("took")))
            self.by_qid[qid] = ms
            if "error" in d:
                self.errors.append({"qid": qid, "error": json.dumps(d["error"])[:300]})
        if "error" in d:
            return []
        return [h["_id"] for h in d["hits"]["hits"]]

    def latency_summary(self, warmup):
        warm = [x for x in self.lat if x[0] > warmup] or self.lat
        cm = [x[1] for x in warm]
        sm = [x[2] for x in warm if x[2] is not None]
        return {"warmup_excluded": min(warmup, len(self.lat)), "n": len(warm),
                "client_ms_p50": pct(cm, 50), "client_ms_p95": pct(cm, 95),
                "client_ms_p99": pct(cm, 99), "server_took_ms_p50": pct(sm, 50),
                "server_took_ms_p95": pct(sm, 95), "server_took_ms_p99": pct(sm, 99),
                "client_ms_mean": round(float(np.mean(cm)), 1) if cm else None}


def add_latency_column(name, by_qid):
    p = RESULTS_DIR / f"{name}.per_query.parquet"
    t = pq.read_table(p)
    lat = [by_qid.get(q) for q in t.column("query_id").to_pylist()]
    pq.write_table(t.append_column("latency_ms", pa.array(lat, pa.float64())), p)


def run(engine, mode, name_idx, concurrency, warmup, limit):
    url = base(engine)
    name = f"{engine}_{mode}{SUFFIX if mode != 'bm25' else ''}"
    version = requests.get(url, timeout=10).json()["version"]["number"]
    eng = f"{'Elasticsearch' if engine == 'es' else 'OpenSearch'} {version}"
    common_cfg = {"engine": eng, "index": name_idx, "mode": mode, "k": DEPTH,
                  "text_analyzer": "standard tokenizer + lowercase + _english_ stop (33 words), no stemming",
                  "bm25": "Lucene BM25Similarity k1=1.2 b=0.75 (1 shard)"}

    if mode == "hybrid_client":
        _, qrels = load_qrels()
        b, v = f"{engine}_bm25", f"{engine}_knn{SUFFIX}"
        fused = fuse_runs([load_run(b), load_run(v)], k=RRF_K, depth=DEPTH)
        fused = {q: fused.get(q, []) for q in qrels}
        save_run(name, fused)
        out = write_results(name, fused, qrels, {**common_cfg, "fusion": "client-side RRF (evallib.rrf)",
                                                  "rrf_k": RRF_K, "inputs": [b, v],
                                                  "input_depth": DEPTH, "output_depth": DEPTH}, {})
        lb = pq.read_table(RESULTS_DIR / f"{b}.per_query.parquet").to_pandas().set_index("query_id")
        lv = pq.read_table(RESULTS_DIR / f"{v}.per_query.parquet").to_pandas().set_index("query_id")
        s = (lb["latency_ms"] + lv["latency_ms"]).dropna()
        out["latency"] = {"definition": "per query: bm25 client ms + knn client ms (sequential client); "
                                        "includes warm-up queries of both input runs",
                          "n": int(len(s)), "client_ms_p50": pct(s.tolist(), 50), "client_ms_p95": pct(s.tolist(), 95),
                          "client_ms_p99": pct(s.tolist(), 99)}
        (RESULTS_DIR / f"{name}.json").write_text(json.dumps(out, indent=2))
        add_latency_column(name, s.to_dict())
        print(json.dumps(out["latency"]))
        return

    native = setup_native(engine) if mode == "hybrid_native" else None
    qvec = load_query_vectors() if mode != "bm25" else None
    client = Client(url, name_idx)

    def one(item):
        qid, q = item
        body, params = query_body(engine, mode, q, qvec[qid] if qvec else None)
        return client.query(qid, body, params)

    pool = ThreadPoolExecutor(concurrency)

    def retriever(chunk):
        return list(pool.map(one, chunk))

    if limit:
        queries, qrels = load_qrels()
        items = list(queries.items())[:limit]
        t0 = time.time()
        res = retriever(items)
        from evallib import evaluate
        summ, _ = evaluate({qid: r for (qid, _), r in zip(items, res)},
                           {qid: qrels[qid] for qid, _ in items})
        print(f"{len(items)} queries in {time.time() - t0:.1f}s; empty={sum(1 for r in res if not r)}; "
              f"errors={client.errors[:3]}; native={native}; metrics={summ}; "
              f"lat={client.latency_summary(0)}")
        for (qid, q), r in list(zip(items, res))[:3]:
            print(qid, q, r[:5])
        return

    cfg = dict(common_cfg, concurrency=concurrency)
    if mode != "bm25":
        cfg["vector_index"] = ("HNSW m=16 ef_construction=100, float32 (no quantisation), dot_product "
                               "on L2-normalised vectors (= cosine), dim 768")
        cfg["knn"] = ({"k": DEPTH, "num_candidates": NUM_CAND} if engine == "es"
                      else {"k": DEPTH, "ef_search": NUM_CAND, "engine": "lucene"})
    if mode == "hybrid_native":
        cfg["fusion"] = ("ES retriever.rrf (rank_window_size=100, rank_constant=60), license=" + native
                         if engine == "es" else
                         "OS hybrid query + score-ranker-processor technique=rrf rank_constant=60, "
                         "pagination_depth=100")
    run_retriever(name, retriever, cfg, k=DEPTH, batch=512)
    pool.shutdown()
    out = RESULTS_DIR / f"{name}.json"
    d = json.loads(out.read_text())
    d["latency"] = client.latency_summary(warmup)
    d["latency"]["note"] = ("from the quality run (concurrency %d, engine alone on the laptop); "
                            "see %s_latency.json for the isolated pass" % (concurrency, engine))
    d["errors"] = {"count": len(client.errors), "sample": client.errors[:5]}
    out.write_text(json.dumps(d, indent=2))
    add_latency_column(name, client.by_qid)
    print(json.dumps({"latency": d["latency"], "errors": d["errors"]["count"]}), flush=True)


def latency(engine, name_idx, n, nwarm, seed, concs, modes):
    """Isolated latency pass: the same N queries as pinot_runs.py --latency (random.Random(seed) sample of
    the full test split), after nwarm untimed warm-up queries from outside the sample, per mode and
    concurrency. hybrid_client = bm25 request then knn request, sequential, per query."""
    import random
    queries, _ = load_qrels()
    ids = sorted(queries)
    sample = random.Random(seed).sample(ids, n)
    warm = random.Random(seed + 1).sample(sorted(set(ids) - set(sample)), nwarm)
    qvec = load_query_vectors()
    url = base(engine)
    out_f = RESULTS_DIR / f"{engine}_latency{SUFFIX}.json"
    out = json.loads(out_f.read_text()) if out_f.exists() else {}
    out["config"] = {"num_candidates_or_ef_search": NUM_CAND, "index": name_idx, "queries": n, "warmup_queries": nwarm, "seed": seed,
                     "version": requests.get(url, timeout=10).json()["version"]["number"],
                     "latency": "client wall ms per query (hybrid_client = bm25 + knn requests in sequence); "
                                "server_took_ms = engine `took` (single-request modes)",
                     "qps": "queries / wall seconds of the measured batch"}
    for mode in modes:
        if mode == "hybrid_native":
            setup_native(engine)
        out.setdefault(mode, {})
        for c in concs:
            client = Client(url, name_idx)
            lat, lock = [], threading.Lock()

            def one(item):
                qid, q = item
                t0 = time.perf_counter()
                if mode == "hybrid_client":
                    a = client.query(qid, *query_body(engine, "bm25", q, None))
                    b = client.query(qid, *query_body(engine, "knn", q, qvec[qid]))
                    res = a + b
                else:
                    res = client.query(qid, *query_body(engine, mode, q, qvec[qid]))
                with lock:
                    lat.append((time.perf_counter() - t0) * 1000)
                return res

            with ThreadPoolExecutor(c) as pool:
                list(pool.map(one, [(q, queries[q]) for q in warm]))
                lat.clear()
                client.lat.clear()
                nerr0 = len(client.errors)
                t0 = time.perf_counter()
                res = list(pool.map(one, [(q, queries[q]) for q in sample]))
                wall = time.perf_counter() - t0
            took = [x[2] for x in client.lat if x[2] is not None]
            summ = {"n": len(lat), "concurrency": c, "client_ms_p50": pct(lat, 50), "client_ms_p95": pct(lat, 95),
                    "client_ms_p99": pct(lat, 99), "client_ms_mean": round(float(np.mean(lat)), 1),
                    "server_took_ms_p50": pct(took, 50) if mode != "hybrid_client" else None,
                    "server_took_ms_p95": pct(took, 95) if mode != "hybrid_client" else None,
                    "server_took_ms_p99": pct(took, 99) if mode != "hybrid_client" else None,
                    "wall_s": round(wall, 2), "qps": round(n / wall, 1),
                    "empty_results": sum(1 for r in res if not r),
                    "errors": len(client.errors) - nerr0, "error_sample": client.errors[nerr0:nerr0 + 3]}
            out[mode][f"c{c}"] = summ
            out_f.write_text(json.dumps(out, indent=2))
            print(mode, json.dumps(summ), flush=True)


VEC_EXT = {"vec", "vex", "vem", "vemf", "veq", "vemq", "veb", "vemb"}


def _server_stats(engine, name):
    url = base(engine)
    st = requests.get(f"{url}/{name}/_stats/indexing,merge,refresh,flush,store,segments,docs",
                      params={"include_segment_file_sizes": "true"}, timeout=120).json()
    pr = st["_all"]["primaries"]
    ix, mg, rf, fl = pr["indexing"], pr["merges"], pr["refresh"], pr["flush"]
    files = pr["segments"].get("file_sizes", {})
    by_ext = {k: v.get("size_in_bytes", 0) for k, v in files.items()}
    vec = sum(v for k, v in by_ext.items() if k in VEC_EXT)
    out = {"note": "cumulative shard stats since node start (index was built in this node session); "
                   "times are summed across threads, not wall time",
           "indexing_index_total": ix["index_total"],
           "indexing_index_time_secs": round(ix["index_time_in_millis"] / 1000, 1),
           "indexing_throttle_time_secs": round(ix.get("throttle_time_in_millis", 0) / 1000, 1),
           "merges_total": mg["total"], "merges_total_time_secs": round(mg["total_time_in_millis"] / 1000, 1),
           "merges_total_size_bytes": mg["total_size_in_bytes"],
           "merges_throttled_time_secs": round(mg.get("total_throttled_time_in_millis", 0) / 1000, 1),
           "refresh_total": rf["total"], "refresh_total_time_secs": round(rf["total_time_in_millis"] / 1000, 1),
           "flush_total": fl["total"], "flush_total_time_secs": round(fl["total_time_in_millis"] / 1000, 1),
           "store_bytes": pr["store"]["size_in_bytes"], "segments": pr["segments"]["count"],
           "docs": pr["docs"]["count"],
           "segment_file_sizes_by_ext": by_ext,
           "size_split_from_file_ext": {"vector_bytes(vec/vex/vem*)": vec,
                                        "other_bytes(text postings, norms, ids, keywords, ...)":
                                            sum(by_ext.values()) - vec} if by_ext else None,
           "hnsw_build_time": "not exposed separately by the stats APIs: HNSW insertion is inside "
                              "indexing_index_time and graph merging inside merges_total_time"}
    if engine == "es":
        du = requests.post(f"{url}/{name}/_disk_usage", params={"run_expensive_tasks": "true"},
                           timeout=3600).json()[name]
        fields = {}
        for f, v in du["fields"].items():
            fields[f] = {k: v.get(k) for k in ("total_in_bytes", "inverted_index", "stored_fields_in_bytes",
                                               "doc_values_in_bytes", "points_in_bytes", "norms_in_bytes",
                                               "term_vectors_in_bytes", "knn_vectors_in_bytes")}
        out["es_disk_usage"] = {"store_size_in_bytes": du["store_size_in_bytes"],
                                "all_fields_total_in_bytes": du["all_fields"]["total_in_bytes"],
                                "text_field_bytes": du["fields"].get("text", {}).get("total_in_bytes"),
                                "embedding_field_bytes": du["fields"].get("embedding", {}).get("total_in_bytes"),
                                "fields": fields}
    return out


def timings(engine, name):
    f = RESULTS_DIR / f"{engine}_timings.json"
    prev = json.loads(f.read_text()) if f.exists() else {}
    ix = json.loads((RESULTS_DIR / f"{engine}_index.json").read_text())
    prep = RESULTS_DIR / f"{engine}_prep_only.json"
    prep = json.loads(prep.read_text()) if prep.exists() else None
    try:
        srv = _server_stats(engine, name)
    except requests.RequestException:
        srv = prev.get("server_stats")
    ingestion = {"bulk_wall_secs": ix["bulk_secs"], "bulk_workers": ix["bulk_workers"],
                 "bulk_docs_per_request": ix["bulk_docs"], "docs": ix["docs_indexed"],
                 "input_parquet_load_secs": ix.get("input_load_secs"),
                 "client_doc_prep_secs_sum": ix.get("doc_prep_secs_sum"),
                 "client_bulk_request_secs_sum": ix.get("bulk_post_secs_sum")}
    if ingestion["client_doc_prep_secs_sum"] is None and prep:
        ingestion["client_doc_prep_secs_sum"] = prep["doc_prep_cpu_secs_sum"]
        ingestion["client_doc_prep_source"] = ("separate --prep-only pass (same code path, no posting), "
                                               f"wall {prep['wall_secs_incl_parquet_read']} s with "
                                               f"{prep['workers']} workers")
    ingestion["note"] = ("doc prep (JSON/ndjson serialisation) runs in the 4 worker processes "
                         "interleaved with posting; *_sum fields are summed over workers, so the "
                         "server-side share of bulk wall time ~= bulk_wall_secs * request_sum / "
                         "(prep_sum + request_sum)")
    if ingestion["client_doc_prep_secs_sum"] and ingestion["client_bulk_request_secs_sum"]:
        p_, q_ = ingestion["client_doc_prep_secs_sum"], ingestion["client_bulk_request_secs_sum"]
        ingestion["est_server_share_of_bulk_wall_secs"] = round(ix["bulk_secs"] * q_ / (p_ + q_), 1)
    indexing = {"bulk_wall_secs": ix["bulk_secs"], "final_refresh_secs": ix["refresh_secs"],
                "merge_settle_secs": ix["merge_settle_secs"], "force_merge_secs": ix.get("force_merge_secs"),
                "force_merge": "max_num_segments=1 after merges settled" if ix.get("force_merge_secs") is not None
                               else "none (not performed)",
                "segments_before_force_merge": ix.get("segments_before_force_merge"),
                "total_incl_force_merge_secs": ix.get("index_secs_total_incl_force_merge"),
                "total_until_searchable_and_merges_settled_secs": ix["index_secs_total"]}
    size = {"store_bytes": ix["store_bytes"], "data_dir_bytes(du)": ix["data_dir_bytes"],
            "segments_after_settle": ix["segments"]}
    if srv:
        size["split"] = srv.get("size_split_from_file_ext")
        if srv.get("es_disk_usage"):
            size["es_disk_usage_text_bytes"] = srv["es_disk_usage"]["text_field_bytes"]
            size["es_disk_usage_embedding_bytes"] = srv["es_disk_usage"]["embedding_field_bytes"]
    query = {}
    for m, _ in ENGINE_MODES:
        r = RESULTS_DIR / f"{engine}_{m}.json"
        if r.exists():
            query[m] = json.loads(r.read_text()).get("latency")
    out = {"engine": engine, "version": ix["version"]["number"], "index": name,
           "context": "engine ran alone (no Pinot cluster, no other search engine) on the laptop, after a Docker Desktop restart",
           "ingestion": ingestion, "indexing": indexing, "index_size": size,
           "query_latency_ms_from_quality_runs": query, "server_stats": srv,
           "written_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    f.write_text(json.dumps(out, indent=2))
    print(json.dumps({k: out[k] for k in ("ingestion", "indexing", "index_size")}, indent=2))


REF_ROWS = [("ref_bm25", "Reference BM25 (bm25s, exact)"),
            ("ref_vector_exact", "Reference vector (faiss flat, exact)"),
            ("ref_hybrid_rrf_bm25", "Reference hybrid RRF (client)")]
ENGINE_MODES = [("bm25", "BM25"), ("knn", "kNN HNSW"), ("hybrid_native", "Hybrid RRF native"),
                ("hybrid_client", "Hybrid RRF client")]

NOTES = """## Notes

- Context: both engines were indexed and queried on one laptop while a Pinot cluster and its eval
  runner (separate experiment) were running and busy, so all times are indicative only.
- Latencies are indicative only: measured on one laptop while another JVM (Pinot, 8 GB heap) was
  busy; client wall time per request at concurrency 8, warm (first 500 requests excluded).
  Client-side RRF latency is the per-query sum of the BM25 and kNN requests.
- Analyzer: ES/OS use the `standard` tokenizer + lowercase + Lucene `_english_` stop list (33 words),
  no stemming. The reference uses bm25s tokenisation (regex word pattern) with bm25s' English stop
  list; both are Lucene BM25 k1=1.2 b=0.75, so small nDCG gaps come from tokenisation/stop-word
  differences, not scoring.
- kNN: HNSW (m=16, ef_construction=100, float32, no quantisation) with num_candidates/ef_search=200
  is approximate; the reference is exact brute force, so the gap is HNSW recall loss.
- Native vs client RRF: both k=60 over top-100 lists. ES retriever.rrf / OS score-ranker rrf fuse
  the same two lists inside the engine; differences vs client RRF come from tie-breaking and from
  per-shard vs global candidate windows.
- Index time = bulk load (4 processes, 500 docs/request) + refresh + wait for background merges to
  settle; no force-merge. Index size = primary store size (_source disabled, _id = product_id).
- Text vs vector size: ES from `_disk_usage?run_expensive_tasks=true` (per field); OS from segment
  file sizes by extension (vec/vex/vem* = vectors, rest = text postings/norms/ids/keywords).
- HNSW build time is not exposed separately by either engine's stats APIs: graph insertion is part of
  `indexing.index_time` and graph merging part of `merges.total_time` (both summed across threads).
"""


def report():
    def load(n):
        f = RESULTS_DIR / f"{n}.json"
        return json.loads(f.read_text()) if f.exists() else None

    rows = ["| System | Mode | nDCG@10 | R@100 | p50 ms | p95 ms | p99 ms | Index time | Index size |",
            "|---|---|---|---|---|---|---|---|---|"]
    for n, label in REF_ROWS:
        d = load(n)
        if d:
            rows.append(f"| {label} | `{n}` | {d['metrics']['ndcg@10']:.4f} | "
                        f"{d['metrics']['recall@100']:.4f} | - | - | - | "
                        f"{d['config'].get('index_secs', '-')}{' s' if 'index_secs' in d['config'] else ''} | - |")
    for eng, ename in (("es", "Elasticsearch"), ("os", "OpenSearch")):
        ix = load(f"{eng}_index")
        it = f"{ix['index_secs_total'] / 60:.1f} min" if ix else "-"
        isz = f"{ix['store_bytes'] / 2**30:.2f} GiB" if ix else "-"
        ver = ix["version"]["number"] if ix else ""
        for m, label in ENGINE_MODES:
            d = load(f"{eng}_{m}")
            if not d:
                rows.append(f"| {ename} {ver} | {label} | not run | | | | | {it} | {isz} |")
                continue
            lat = d.get("latency", {})
            rows.append(f"| {ename} {ver} | {label} | {d['metrics']['ndcg@10']:.4f} | "
                        f"{d['metrics']['recall@100']:.4f} | {lat.get('client_ms_p50', '-')} | "
                        f"{lat.get('client_ms_p95', '-')} | {lat.get('client_ms_p99', '-')} | {it} | {isz} |")
    trows = ["| Engine | Bulk wall (ingest) | client prep (sum over 4 workers) | bulk requests (sum) "
             "| final refresh | merge settle | force-merge | total until searchable | store | text | vector "
             "| idx time (stats, thread-sum) | merge time (stats) | refresh time (stats) |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    gib = lambda b: f"{b / 2**30:.2f} GiB" if b else "-"  # noqa: E731
    for eng, ename in (("es", "Elasticsearch"), ("os", "OpenSearch")):
        t = load(f"{eng}_timings")
        if not t:
            continue
        ig, ixx, sz, sv = t["ingestion"], t["indexing"], t["index_size"], t.get("server_stats") or {}
        text_b = sz.get("es_disk_usage_text_bytes")
        vec_b = sz.get("es_disk_usage_embedding_bytes")
        if vec_b is None and sz.get("split"):
            vals = list(sz["split"].values())
            vec_b, text_b = vals[0], vals[1]
        trows.append(f"| {ename} {t['version']} | {ig['bulk_wall_secs']} s | {ig.get('client_doc_prep_secs_sum')} s | "
                     f"{ig.get('client_bulk_request_secs_sum') or '-'} s | {ixx['final_refresh_secs']} s | "
                     f"{ixx['merge_settle_secs']} s | none | {ixx['total_until_searchable_and_merges_settled_secs']} s | "
                     f"{gib(sz['store_bytes'])} | {gib(text_b)} | {gib(vec_b)} | "
                     f"{sv.get('indexing_index_time_secs', '-')} s | {sv.get('merges_total_time_secs', '-')} s | "
                     f"{sv.get('refresh_total_time_secs', '-')} s |")
    md = ("# ES / OpenSearch baselines (ESCI US test, 22,458 queries)\n\n"
          "Generated by `search_baselines.py report`. R@100 = Exact-only recall. Latency = client ms from "
          "the quality runs (concurrency 8, indicative; an isolated one-at-a-time pass follows).\n\n"
          + "\n".join(rows) + "\n\n## Ingestion / indexing / size (results/<es|os>_timings.json)\n\n"
          + "\n".join(trows) + "\n\n" + NOTES)
    (RESULTS_DIR / "baselines.md").write_text(md)
    print(md)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=list(ENGINES), required=True)
    ap.add_argument("cmd", choices=["up", "down", "index", "run", "report", "timings", "latency"])
    ap.add_argument("--lat-n", type=int, default=1000)
    ap.add_argument("--lat-warm", type=int, default=200)
    ap.add_argument("--lat-conc", default="1,8")
    ap.add_argument("--lat-modes", default="bm25,knn,hybrid_native,hybrid_client")
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--mode", choices=["bm25", "knn", "hybrid_native", "hybrid_client"])
    ap.add_argument("--heap", default="4g")
    ap.add_argument("--mem", default="9g")
    ap.add_argument("--index", default=INDEX)
    ap.add_argument("--limit-docs", type=int, default=0)
    ap.add_argument("--prep-only", action="store_true", help="time client doc prep, no posting")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--warmup", type=int, default=500)
    ap.add_argument("--limit", type=int, default=0, help="smoke test: first N queries, no files")
    ap.add_argument("--num-cand", type=int, default=200,
                    help="ES num_candidates / OS ef_search; != 200 adds suffix _c<N> to result names")
    a = ap.parse_args()
    global NUM_CAND, SUFFIX
    NUM_CAND = a.num_cand
    SUFFIX = "" if a.num_cand == 200 else f"_c{a.num_cand}"
    if a.cmd == "up":
        up(a.engine, a.heap, a.mem)
    elif a.cmd == "down":
        down(a.engine)
    elif a.cmd == "index":
        index(a.engine, a.index, a.limit_docs, a.workers, a.prep_only)
    elif a.cmd == "timings":
        timings(a.engine, a.index)
    elif a.cmd == "latency":
        latency(a.engine, a.index, a.lat_n, a.lat_warm, a.seed, [int(x) for x in a.lat_conc.split(",")],
                a.lat_modes.split(","))
    elif a.cmd == "report":
        report()
    else:
        run(a.engine, a.mode, a.index, a.concurrency, a.warmup, a.limit)


if __name__ == "__main__":
    main()
