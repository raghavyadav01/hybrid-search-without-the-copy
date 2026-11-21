"""Onboarding / index-build timings and per-index sizes for esci_et and esci_local
-> results/pinot_timings.json.

  .venv/bin/python -I collect_timings.py

Sources (all from this session's runs, see stp-build.md "ESCI tables"):
- esci_et: the clean onboarding run (cluster log stp-cache/run/quickstart.attempt4.log; sync
  scheduled 19:20:29, nothing else heavy running on the cluster). Per segment: text/HNSW build
  from the server's "Creating new text index"/"Created text index" and "Creating new Vector
  index"/"Created Vector index" lines; load = "Adding new ONLINE segment" -> "Added new immutable
  segment"; registration = sync start -> "Added 33 segments" (controller, metadata only).
- esci_local: standalone ingestion job logs (stp-cache/run/local/ingest.log = batch 1, 15 files,
  3 parallel threads, no other load; ingest2.log = batch 2, 18 files, ran concurrently with the
  ET re-sync after the 20:11 restart -> contended). Per segment: record indexing incl. HNSW graph
  insertion ("Creating HNSW index" -> "Sealing HNSW"), HNSW seal/merge ("Sealing HNSW" -> "Packed
  HNSW"), text index finish ("lucene doc IdMapping" -> "Successfully combined"), whole segment
  (first log line of the file's work -> "Successfully created segment"), tar; server load from the
  current cluster log ("Adding new ONLINE segment" -> "Added new immutable segment").
- Sizes: per-index bytes summed over every segment from the servers' segment-metadata API.
"""
import json
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import requests

from common import RESULTS_DIR

RUN = Path(__file__).resolve().parent / "stp-cache" / "run"
ET_LOG = RUN / "quickstart.attempt4.log"
CUR_LOG = RUN / "quickstart.log"
TS = re.compile(r"^(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d{3}) ")


def ts(line):
    m = TS.match(line)
    return datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S.%f").timestamp() if m else None


def stats(xs):
    xs = [x for x in xs if x is not None]
    if not xs:
        return None
    return {"n": len(xs), "mean_s": round(statistics.mean(xs), 1), "p50_s": round(statistics.median(xs), 1),
            "max_s": round(max(xs), 1), "sum_s": round(sum(xs), 1)}


def et_timings():
    ev = defaultdict(dict)
    sync_start = registered = first_gen = last_gen = None
    seg_re = re.compile(r"segment:? ([0-9a-f]{32})")
    for line in open(ET_LOG, errors="replace"):
        if "esci_et" not in line and "segment" not in line:
            continue
        t = ts(line)
        if t is None:
            continue
        if sync_start is None and "esci_et_OFFLINE_controller_" in line:
            sync_start = t
        if "Generating segment:" in line and "ExternalTableControllerBasedIngestion" in line:
            first_gen = first_gen or t
        if "Uploaded segment:" in line and "ExternalTableControllerBasedIngestion" in line:
            last_gen = t
        if registered is None and "Added 33 segments" in line:
            registered = t
        m = seg_re.search(line)
        if not m:
            continue
        s = m.group(1)
        for key, pat in (("load_start", "Adding new ONLINE segment"),
                         ("text_start", "Creating new text index"), ("text_end", "Created text index"),
                         ("vec_start", "Creating new Vector index"), ("vec_end", "Created Vector index"),
                         ("tier_upload_end", "from local indexDir"),
                         ("load_end", "Added new immutable segment")):
            if pat in line and key not in ev[s]:
                ev[s][key] = t
    segs = {s: e for s, e in ev.items() if "load_start" in e}
    d = lambda e, a, b: (e[b] - e[a]) if a in e and b in e else None  # noqa: E731
    all_end = max(e.get("load_end", 0) for e in segs.values())
    return {
        "method": "clean onboarding run 19:20 (cluster log quickstart.attempt4.log); 3 servers in one JVM, "
                  "segment loads in parallel (Helix state-transition threads)",
        "segments": len(segs),
        "sync_scheduled_epoch": sync_start,
        "catalog_sync_registration_s": round(registered - sync_start, 1),
        "metadata_segment_generation_s": round(last_gen - first_gen, 1) if first_gen and last_gen else None,
        "per_segment_text_index_build": stats([d(e, "text_start", "text_end") for e in segs.values()]),
        "per_segment_hnsw_build": stats([d(e, "vec_start", "vec_end") for e in segs.values()]),
        "per_segment_load_total(build+upload-to-tier+open)": stats([d(e, "load_start", "load_end")
                                                                    for e in segs.values()]),
        "index_build_wall_s(first load start -> last segment loaded)":
            round(all_end - min(e["load_start"] for e in segs.values()), 1),
        "time_to_fully_queryable_s(sync scheduled -> 33/33 ONLINE in external view)": 597,
    }


def local_job(log):
    per = []
    threads = defaultdict(dict)
    for line in open(log, errors="replace"):
        m = re.search(r"\[(pool-3-thread-\d+)\]", line)
        t = ts(line)
        if not m or t is None:
            continue
        th = threads[m.group(1)]
        th.setdefault("start", t)
        if "Creating HNSW index" in line:
            th["hnsw_start"] = t
        elif "Sealing HNSW" in line:
            th["hnsw_seal"] = t
        elif "Packed HNSW index" in line:
            th["hnsw_packed"] = t
        elif "lucene doc IdMapping" in line:
            th["text_finish_start"] = t
        elif "LuceneTextIndexCombined" in line and "Successfully combined" in line:
            th["text_finish_end"] = t
        elif "Successfully created segment" in line:
            th["created"] = t
        elif "Size for segment" in line:
            th["tarred"] = t
            per.append(dict(th))
            threads[m.group(1)] = {"start": t}
    g = lambda a, b: [p[b] - p[a] for p in per if a in p and b in p]  # noqa: E731
    first = min(p["start"] for p in per) if per else None
    last = max(p["tarred"] for p in per) if per else None
    return {"segments": len(per), "wall_s": round(last - first, 1) if per else None,
            "per_segment_total_create": stats(g("start", "created")),
            "per_segment_record_indexing_incl_hnsw_insert_and_text_docs": stats(g("hnsw_start", "hnsw_seal")),
            "per_segment_hnsw_seal_merge": stats(g("hnsw_seal", "hnsw_packed")),
            "per_segment_text_index_finish": stats(g("text_finish_start", "text_finish_end")),
            "per_segment_tar": stats(g("created", "tarred"))}


def local_server_load():
    ev = defaultdict(dict)
    for line in open(CUR_LOG, errors="replace"):
        m = re.search(r"(Adding new ONLINE segment|Added new immutable segment): (esci_local_OFFLINE_\S+)", line)
        if m:
            ev[m.group(2)].setdefault("start" if m.group(1).startswith("Adding") else "end", ts(line))
    return stats([e["end"] - e["start"] for e in ev.values() if "start" in e and "end" in e])


def index_sizes(table):
    servers = [17500, 17501, 17502]
    tot = defaultdict(lambda: defaultdict(int))
    nseg = 0
    for p in servers:
        try:
            segs = requests.get(f"http://localhost:{p}/tables/{table}_OFFLINE/segments", timeout=30).json()["segments"]
        except Exception:  # noqa: BLE001
            continue
        for s in segs:
            md = requests.get(f"http://localhost:{p}/tables/{table}_OFFLINE/segments/{s}/metadata?columns=*",
                              timeout=60).json()
            nseg += 1
            for c in md.get("columns", []):
                for idx, size in (c.get("indexSizeMap") or {}).items():
                    tot[c["columnName"]][idx] += int(size)
    by_type = defaultdict(int)
    for col, m in tot.items():
        for idx, size in m.items():
            by_type[idx] += size
    return {"segments": nseg, "bytes_by_index_type": dict(by_type),
            "bytes_by_column_and_index": {c: dict(m) for c, m in tot.items()}}


def main():
    out = {"generated": datetime.now().isoformat(timespec="seconds")}
    out["esci_et"] = et_timings()
    b1, b2 = local_job(RUN / "local" / "ingest.log"), local_job(RUN / "local" / "ingest2.log")
    out["esci_local"] = {
        "method": "standalone SegmentCreationAndMetadataPush job, 3 threads, reads the 33 Parquet files from "
                  "MinIO (S3PinotFS); run in two batches because of disk space (batch 2 overlapped the ET "
                  "re-sync); server download+untar+load from the cluster log",
        "batch1_15_segments_uncontended": b1,
        "batch2_18_segments_contended": b2,
        "segment_creation_wall_s_total": round((b1["wall_s"] or 0) + (b2["wall_s"] or 0), 1),
        "per_segment_server_load(download+untar+open)": local_server_load(),
        "metadata_push_and_load_after_creation_s": "<5 (all 18 batch-2 segments ONLINE within 1 s of job exit; "
                                                   "push is per segment during the job)",
    }
    for t in ("esci_et", "esci_local"):
        out[t]["index_sizes"] = index_sizes(t)
    out["notes"] = [
        "esci_et forward index = the Iceberg Parquet files themselves (0 bytes in Pinot); its segments hold only "
        "the text + HNSW indexes, null vectors and metadata (columns.psf on the S3 tier, preloaded to local disk).",
        "Machine: 12-core laptop, 32 GB RAM, heavily memory-overcommitted (Docker VM ~24 GB incl. an unrelated "
        "StarTree CE stack); all timings are indicative.",
    ]
    (RESULTS_DIR / "pinot_timings.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2)[:6000])


if __name__ == "__main__":
    main()
