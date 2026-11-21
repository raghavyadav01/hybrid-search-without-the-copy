"""Quick latency probe on whatever part of esci_1tbb is online: brand-filtered and unfiltered vector search."""
import json, time, requests
B = "http://localhost:18000/query/sql"
def q(sql, opts="timeoutMs=60000"):
    t = time.perf_counter(); d = requests.post(B, json={"sql": sql, "queryOptions": opts}).json()
    ms = (time.perf_counter() - t) * 1000
    if d.get("exceptions"): return ms, d["exceptions"][0].get("message", "")[:150], d
    return ms, None, d
_, _, d = q("select brand, embedding from esci_1tbb where brand is not null limit 1", "timeoutMs=60000;enableNullHandling=true")
brand, emb = d["resultTable"]["rows"][0]
vec = "ARRAY[" + ",".join(f"{x:.6f}" for x in emb) + "]"
b = brand.replace("'", "''")
cases = {
  "brand vector": f"select product_id, VECTOR_DISTANCE(embedding, {vec}, 100) d from esci_1tbb where brand = '{b}' and VECTOR_SIMILARITY(embedding, {vec}, 100) order by d limit 100",
  "brand text": f"select product_id from esci_1tbb where brand = '{b}' and TEXT_MATCH(text, 'black') limit 100",
  "unfiltered vector": f"select product_id, VECTOR_DISTANCE(embedding, {vec}, 100) d from esci_1tbb where VECTOR_SIMILARITY(embedding, {vec}, 100) order by d limit 100",
}
for name, sql in cases.items():
    times = []
    for i in range(6):
        ms, err, d = q(sql); times.append(ms)
        if err: print(name, "ERROR", err); break
    st = {k: d.get(k) for k in ("numSegmentsQueried", "numSegmentsProcessed", "numSegmentsMatched", "numDocsScanned", "timeUsedMs")}
    print(f"{name:18s} brand={brand!r} ms(first,then)={round(times[0])},{[round(x) for x in times[1:]]} {st}")
