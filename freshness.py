"""Freshness experiment: commit -> searchable for an append of 10,000 NEW products to esci.products_us_pi.

  .venv/bin/python -I freshness.py et     --runs 1,2,3      (Pinot External Table; cluster up, esci_et ONLINE)
  .venv/bin/python -I freshness.py local  --runs 4,5,6      (Pinot local copy; cluster up, esci_local ONLINE)
  .venv/bin/python -I freshness.py es     --runs 7,8,9      (Elasticsearch up, esci_products indexed)
  .venv/bin/python -I freshness.py os     --runs 10,11,12   (OpenSearch up, esci_products indexed)
  .venv/bin/python -I freshness.py rollback                 (restore the table's original snapshot)

Append (per run r): 10,000 real rows copied from the table's first data file (rows (r % 3) * 10,000 ...),
product_id -> "FRSH<rr><iiiii>", text += " frsh<rr><iiiii>" (a unique BM25 token per row), embedding
unchanged; written as ONE Parquet file (same writer settings as build_table_pi.py: page index, 64 KB
pages, zstd) and committed by pyiceberg as one new snapshot (fast append) via the REST catalog.
t0 = when the commit returns.

Searchable = COUNT(*) for each of 50 sampled tokens returns 1 AND the total count equals the expected
row count; polled every 1 s.
- Pinot ET: the External Table has no periodic sync configured in this setup (task config without a
  `schedule`; controller.task.scheduler not enabled), so the sync is triggered right after the commit
  (POST /tasks/schedule), the cheapest a commit hook can do; with a cron period P, add U(0, P).
  Breakdown from 1 s polling: sync trigger -> new segment in IdealState (registration), -> ONLINE
  (segment load incl. text + HNSW index build), -> all tokens searchable.
- Pinot local: a SegmentCreationAndMetadataPush job over the new file only (segment name postfix), then
  load; breakdown: job wall (JVM start, read from MinIO, segment + index build, tar, push), -> searchable.
- ES / OS: the copy pipeline given the change set for free (no CDC/schedule lag modelled): read the new
  data file from MinIO, bulk index (same mapping, 4 workers, 500 docs/request), refresh, -> searchable.
  The appended docs are deleted again after each run (delete_by_query + refresh) so the index stays at
  the Table-1 state.
Writes results/freshness.json (merged per system).
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import requests

import build_table_pi  # noqa: F401  (patches the pyiceberg Parquet writer: page index, dictionary cols)
from build_table import catalog
from common import RESULTS_DIR

TABLE = os.environ.get("FRESH_TABLE", "esci.products_us_pi")
PINOT_TABLE = os.environ.get("FRESH_PINOT_TABLE", "esci_et")
N_TOKENS = int(os.environ.get("FRESH_TOKENS", "50"))
BASE_ROWS = 1_215_854
N = 10_000
OUT = RESULTS_DIR / "freshness.json"
STATE = RESULTS_DIR / "freshness_state.json"
CTRL, BROKER = "http://localhost:19000", "http://localhost:18000"
EVAL = Path(__file__).resolve().parent
RUN = EVAL / "stp-cache" / "run"


def fs():
    if os.environ.get("ESCI_EC2"):
        return pafs.S3FileSystem(region=os.environ.get("AWS_REGION", "us-west-2"))
    return pafs.S3FileSystem(endpoint_override="http://localhost:9020", access_key="minioadmin",
                             secret_key="minioadmin", region="us-east-1", scheme="http")


def now():
    return time.time()


def load_state():
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save_result(system, entry):
    d = json.loads(OUT.read_text()) if OUT.exists() else {}
    d.setdefault(system, {"runs": []})
    d[system]["runs"] = [r for r in d[system]["runs"] if r["run"] != entry["run"]] + [entry]
    runs = d[system]["runs"]
    keys = [k for k in runs[0] if k.endswith("_s")]
    d[system]["median"] = {k: round(float(np.median([r[k] for r in runs if r.get(k) is not None])), 2)
                           for k in keys if any(r.get(k) is not None for r in runs)}
    OUT.write_text(json.dumps(d, indent=2))


def append(run):
    """Commits one 10k-row file as a new snapshot. Returns (t0, info)."""
    cat = catalog()
    tbl = cat.load_table(TABLE)
    st = load_state()
    if "original_snapshot_id" not in st:
        st["original_snapshot_id"] = tbl.current_snapshot().snapshot_id
        STATE.write_text(json.dumps(st, indent=2))
    files_df = tbl.inspect.files().to_pandas()
    # the first original data file (skip small files appended by earlier runs)
    first = sorted(files_df[files_df.record_count >= 3 * N].file_path)[0].replace("s3://", "")
    src = pq.read_table(first, filesystem=fs(), partitioning=None)
    off = (run % 3) * N
    src = src.slice(off, N)
    ids = [f"FRSH{run:02d}{i:05d}" for i in range(N)]
    toks = [f"frsh{run:02d}{i:05d}" for i in range(N)]
    text = [t + " " + k for t, k in zip(src.column("text").to_pylist(), toks)]
    schema = tbl.schema().as_arrow()
    cols = []
    for f in schema:
        if f.name == "product_id":
            cols.append(pa.array(ids, pa.string()))
        elif f.name == "text":
            cols.append(pa.array(text, pa.string()))
        else:
            cols.append(src.column(f.name).cast(f.type))
    new = pa.Table.from_arrays(cols, schema=schema)
    before = {f for f in tbl.inspect.files().to_pandas().file_path}
    tc = now()
    tbl.append(new)
    t0 = now()
    tbl = cat.load_table(TABLE)
    files = tbl.inspect.files().to_pandas()
    newf = sorted(set(files.file_path) - before)
    info = {"run": run, "snapshot_id": tbl.current_snapshot().snapshot_id, "new_files": newf,
            "commit_wall_s": round(t0 - tc, 2), "table_rows_after": int(files.record_count.sum()),
            "sample_tokens": random.Random(run).sample(toks, N_TOKENS), "id_prefix": f"FRSH{run:02d}"}
    print(json.dumps({k: v for k, v in info.items() if k != "sample_tokens"}), flush=True)
    return t0, info


# ---------------------------------------------------------------- Pinot
def pinot_count(sql):
    try:
        d = requests.post(BROKER + "/query/sql", json={"sql": sql}, timeout=30).json()
        if d.get("exceptions"):
            return None
        return d["resultTable"]["rows"][0][0]
    except Exception:  # noqa: BLE001
        return None


def pinot_searchable(table, info):
    tot = pinot_count(f"SELECT COUNT(*) FROM {table}")
    if tot != info["table_rows_after"]:
        return False, tot
    for t in info["sample_tokens"]:
        if pinot_count(f"SELECT COUNT(*) FROM {table} WHERE TEXT_MATCH(text, '{t}')") != 1:
            return False, tot
    return True, tot


def view(table, kind):
    try:
        d = requests.get(f"{CTRL}/tables/{table}/{kind}?tableType=OFFLINE", timeout=10).json()["OFFLINE"] or {}
        return d
    except Exception:  # noqa: BLE001
        return {}


def poll_pinot(table, info, t0, segs_before, timeout=3600):
    marks = {}
    while now() - t0 < timeout:
        is_ = view(table, "idealstate")
        ev = view(table, "externalview")
        new = set(is_) - segs_before
        if new and "registered" not in marks:
            marks["registered"] = now()
        if new and all("ONLINE" in (ev.get(s) or {}).values() for s in new) and "online" not in marks:
            marks["online"] = now()
        ok, tot = pinot_searchable(table, info)
        if ok:
            marks["searchable"] = now()
            return marks, sorted(new)
        time.sleep(1)
    return marks, None


def run_et(run):
    segs_before = set(view(PINOT_TABLE, "idealstate"))
    t0, info = append(run)
    ts = now()
    r = requests.post(f"{CTRL}/tasks/schedule", params={"taskType": "ExternalTableSyncTask",
                                                         "tableName": PINOT_TABLE + "_OFFLINE"}, timeout=600)
    te = now()
    marks, new = poll_pinot(PINOT_TABLE, info, t0, segs_before)
    f = lambda k: round(marks[k] - t0, 2) if k in marks else None  # noqa: E731
    entry = {"run": run, **{k: v for k, v in info.items() if k != "sample_tokens"},
             "sync_trigger": "POST /tasks/schedule right after commit (no periodic schedule configured)",
             "trigger_call_s": round(te - ts, 2), "schedule_response": r.text[:200],
             "commit_to_registered_s": f("registered"),
             "commit_to_segment_online_s": f("online"),
             "segment_load_and_index_build_s": round(marks["online"] - marks["registered"], 2)
             if "online" in marks and "registered" in marks else None,
             "commit_to_searchable_s": f("searchable"), "new_segments": new}
    print(json.dumps(entry), flush=True)
    save_result("pinot_et", entry)


def run_local(run):
    segs_before = set(view("esci_local", "idealstate"))
    t0, info = append(run)
    fname = info["new_files"][0].rsplit("/", 1)[-1]
    spec = (EVAL / "pinot" / "esci_local.ingestion-job.pi.yaml").read_text()
    spec = spec.replace("includeFileNamePattern: glob:**/*.parquet", f"includeFileNamePattern: glob:**/{fname}")
    spec = spec.replace("outputDirURI: file://$EVAL_HOME/stp-cache/run/local/segments/",
                        f"outputDirURI: file://$EVAL_HOME/stp-cache/run/local/fresh{run}/")
    spec = spec.replace("segmentCreationJobParallelism: 3", "segmentCreationJobParallelism: 1\nsegmentNameGeneratorSpec:\n"
                        f"  type: simple\n  configs:\n    segment.name.postfix: fr{run}")
    sp = RUN / "local" / f"fresh{run}.yaml"
    sp.write_text(spec)
    cp = (EVAL / "stp-cache" / "startree-tools.full.cp").read_text().strip()
    stp = "$STARTREE_PINOT_HOME"
    cmd = ["/opt/homebrew/opt/openjdk@25/bin/java", "-Xmx4g",
           "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
           "--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.util=ALL-UNNAMED",
           "-Daws.endpointUrlS3=http://127.0.0.1:9020",
           "-cp", f"{RUN}/conf:{stp}/startree-tools/target/classes:{cp}",
           "ai.startree.pinot.tools.admin.StarTreePinotAdministrator", "LaunchDataIngestionJob",
           "-jobSpecFile", str(sp)]
    ts = now()
    with open(RUN / "local" / f"fresh{run}.log", "w") as lf:
        rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT).returncode
    tj = now()
    marks, new = poll_pinot("esci_local", info, t0, segs_before)
    f = lambda k: round(marks[k] - t0, 2) if k in marks else None  # noqa: E731
    entry = {"run": run, **{k: v for k, v in info.items() if k != "sample_tokens"}, "job_rc": rc,
             "ingestion_job_wall_s": round(tj - ts, 2),
             "commit_to_job_start_s": round(ts - t0, 2),
             "job_end_to_searchable_s": round(marks["searchable"] - tj, 2) if "searchable" in marks else None,
             "commit_to_searchable_s": f("searchable"), "new_segments": new}
    print(json.dumps(entry), flush=True)
    save_result("pinot_local", entry)


# ---------------------------------------------------------------- ES / OS
def run_search(engine, run):
    import search_baselines as sb
    url = sb.base(engine)
    name = sb.INDEX
    t0, info = append(run)
    tr = now()
    t = pq.read_table(info["new_files"][0].replace("s3://", ""), filesystem=fs(), partitioning=None,
                      columns=["product_id", "text", "brand", "color", "embedding"])
    ids = t.column("product_id").to_pylist()
    texts, brands, colors = (t.column(c).to_pylist() for c in ("text", "brand", "color"))
    emb = t.column("embedding").combine_chunks().flatten().to_numpy(zero_copy_only=False).reshape(-1, 768).astype(np.float32)
    trd = now()
    nerr = 0
    with ProcessPoolExecutor(4) as ex:
        futs = [ex.submit(sb._post_bulk, (url, name, ids[i:i + sb.BULK_DOCS], texts[i:i + sb.BULK_DOCS],
                                          brands[i:i + sb.BULK_DOCS], colors[i:i + sb.BULK_DOCS],
                                          emb[i:i + sb.BULK_DOCS], False))
                for i in range(0, len(ids), sb.BULK_DOCS)]
        for fu in futs:
            nerr += fu.result()[2]
    tb = now()
    requests.post(f"{url}/{name}/_refresh", timeout=600).raise_for_status()
    tf = now()

    def searchable():
        c = requests.get(f"{url}/{name}/_count", timeout=30).json().get("count")
        if c != BASE_ROWS + N:
            return False
        for tok in info["sample_tokens"]:
            r = requests.post(f"{url}/{name}/_count", json={"query": {"match": {"text": tok}}}, timeout=30).json()
            if r.get("count") != 1:
                return False
        return True

    while not searchable() and now() - t0 < 3600:
        time.sleep(1)
    ts = now()
    entry = {"run": run, **{k: v for k, v in info.items() if k != "sample_tokens"}, "bulk_errors": nerr,
             "pipeline": "read new data file from MinIO -> bulk (4 workers, 500 docs) -> refresh; change set "
                         "handed over at commit (no CDC/schedule lag modelled)",
             "commit_to_read_start_s": round(tr - t0, 2), "read_s": round(trd - tr, 2), "bulk_s": round(tb - trd, 2),
             "refresh_s": round(tf - tb, 2), "refresh_to_searchable_s": round(ts - tf, 2),
             "commit_to_searchable_s": round(ts - t0, 2)}
    print(json.dumps(entry), flush=True)
    save_result("es" if engine == "es" else "os", entry)
    # restore the Table-1 index state
    requests.post(f"{url}/{name}/_delete_by_query", params={"refresh": "true", "conflicts": "proceed"},
                  json={"query": {"prefix": {"product_id": "FRSH"}}}, timeout=3600)


def rollback():
    st = load_state()
    cat = catalog()
    tbl = cat.load_table(TABLE)
    orig = st["original_snapshot_id"]
    if tbl.current_snapshot().snapshot_id != orig:
        tbl.manage_snapshots().set_current_snapshot(orig).commit()
    tbl = cat.load_table(TABLE)
    files = tbl.inspect.files().to_pandas()
    # remove orphan data files of the rolled-back appends (keeps glob-based loaders exact)
    live = {f.replace("s3://", "") for f in files.file_path}
    f_ = fs()
    data_dir = (os.environ["ESCI_BUCKET"] + "/warehouse" if os.environ.get("ESCI_EC2") else "esci-warehouse") + \
        "/esci/products_us_pi/data/locale=us"
    orphans = [i.path for i in f_.get_file_info(pafs.FileSelector(data_dir)) if i.path not in live]
    for o in orphans:
        f_.delete_file(o)
    print("deleted orphan files:", len(orphans))
    print(json.dumps({"current_snapshot_id": tbl.current_snapshot().snapshot_id, "rows": int(files.record_count.sum()),
                      "data_files": len(files)}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("system", choices=["et", "local", "es", "os", "rollback", "append-only"])
    ap.add_argument("--runs", default="1")
    a = ap.parse_args()
    if a.system == "rollback":
        return rollback()
    for r in (int(x) for x in a.runs.split(",")):
        if a.system == "et":
            run_et(r)
        elif a.system == "local":
            run_local(r)
        elif a.system in ("es", "os"):
            run_search(a.system, r)
        else:
            append(r)
        rollback() if a.system in ("es", "os") else None


if __name__ == "__main__":
    main()
