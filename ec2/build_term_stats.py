"""Materialize BM25 corpus statistics for the whole vocabulary of the lake (refreshed once per snapshot).

  python3 build_term_stats.py PINOT_HOST TABLE OUT.json [BATCH]

Tokenizes the product text of the original catalog (replicas carry the same text, so it is the lake's vocabulary)
into plain lowercase ASCII words, drops the analyzer's stop words, and asks Pinot for TEXT_SCORE_STATS in batches of
terms. Writes {"coll": {...}, "terms": {term: [docFreq, totalTermFreq]}, "seconds": ...}.
"""
import json, re, sys, time
from concurrent.futures import ThreadPoolExecutor
import pyarrow.fs as pafs, pyarrow.parquet as pq, requests
from pathlib import Path

host, table, out = sys.argv[1], sys.argv[2], sys.argv[3]
batch = int(sys.argv[4]) if len(sys.argv) > 4 else 1000
STOP = set("a an and are as at be but by for if in into is it no not of on or such that the their then there these "
           "they this to was will with".split())
fs = pafs.S3FileSystem(region="us-west-2")
vocab = set()
for f in open(Path.home() / "lake_files.txt"):
    for t in pq.read_table(f.strip().replace("s3://", ""), filesystem=fs, columns=["text"], partitioning=None) \
            .column("text").to_pylist():
        vocab.update(re.findall(r"[a-z0-9]+", (t or "").lower()))
vocab -= STOP
terms = sorted(vocab)
print("vocabulary", len(terms), flush=True)
url = f"http://{host}:18000/query/sql"
s = requests.Session()
t0 = time.time()

def stats(chunk):
    sql = f"SELECT TEXT_SCORE_STATS(text, '{' '.join(chunk)}') FROM {table}"
    d = s.post(url, json={"sql": sql, "queryOptions": "timeoutMs=300000"}).json()
    return json.loads(d["resultTable"]["rows"][0][0])["fields"]["text"]

chunks = [terms[i:i + batch] for i in range(0, len(terms), batch)]
res = {"coll": None, "terms": {}}
with ThreadPoolExecutor(4) as ex:
    for i, d in enumerate(ex.map(stats, chunks)):
        res["coll"] = {k: d[k] for k in ("maxDoc", "docCount", "sumTotalTermFreq", "sumDocFreq")}
        res["terms"].update(d.get("terms", {}))
        if i % 50 == 0:
            print("batch", i, "/", len(chunks), round(time.time() - t0), "s", flush=True)
res["seconds"] = round(time.time() - t0, 1)
res["vocabulary"] = len(terms)
Path(out).write_text(json.dumps(res))
print(json.dumps({"vocabulary": len(terms), "terms_with_stats": len(res["terms"]), "seconds": res["seconds"]}))
