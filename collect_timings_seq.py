"""Onboarding / ingestion / index-build timings and index sizes for the sequential (one system at a time)
runs over esci.products_us_pi -> results/pinot_et_timings.json or results/pinot_local_timings.json.

  .venv/bin/python -I collect_timings_seq.py --table et    --log <cluster log> --watch <watch.jsonl> --t0 <epoch>
  .venv/bin/python -I collect_timings_seq.py --table local --log <cluster log> --watch <watch.jsonl>
                                             --job-log <ingestion job log> --t0 <epoch> [--job-end <epoch>]

--t0: ET = when the ExternalTableSyncTask was scheduled; local = when the ingestion job started.
--watch: pinot/watch_table.sh output (IdealState / ExternalView ONLINE counts every 2 s), which gives
registration time (all segments in the IdealState) and time to fully queryable (all ONLINE).
Per-segment index build times come from the server log lines (see collect_timings.py) and sizes from
the servers' segment-metadata API (cluster must be up).
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

import collect_timings as ct
from common import RESULTS_DIR


def watch_marks(path, t0):
    rows = [json.loads(x) for x in open(path) if x.strip()]
    rows = [r for r in rows if r["t"] >= t0 - 1]
    nmax = max((r["is"] for r in rows), default=0)
    reg = next((r["t"] for r in rows if r["is"] == nmax and nmax > 0), None)
    first_on = next((r["t"] for r in rows if r["ev_online"] > 0), None)
    all_on = next((r["t"] for r in rows if r["ev_online"] == nmax and nmax > 0), None)
    f = lambda x: round(x - t0, 1) if x else None  # noqa: E731
    return {"segments": nmax, "all_segments_in_idealstate_s": f(reg), "first_segment_online_s": f(first_on),
            "all_segments_online_s(time to fully queryable)": f(all_on), "poll_interval_s": 2}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", choices=["et", "local"], required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--watch", required=True)
    ap.add_argument("--t0", type=float, required=True)
    ap.add_argument("--job-log")
    ap.add_argument("--job-end", type=float)
    a = ap.parse_args()
    ct.ET_LOG = Path(a.log)
    ct.CUR_LOG = Path(a.log)
    out = {"generated": datetime.now().isoformat(timespec="seconds"), "lake_table": "esci.products_us_pi",
           "context": "Pinot cluster (1 JVM: ZK, controller, broker, 3 servers, minion; -Xmx8g) alone on the "
                      "laptop; MinIO + Iceberg REST in Docker"}
    marks = watch_marks(a.watch, a.t0)
    if a.table == "et":
        et = ct.et_timings()
        et.pop("time_to_fully_queryable_s(sync scheduled -> 33/33 ONLINE in external view)", None)
        et["method"] = ("sync scheduled at t0; per-segment text/HNSW build from server log; registration and "
                        "queryable times from IdealState/ExternalView polling every 2 s")
        et["sync_scheduled_epoch"] = a.t0
        et.update({"watch_" + k: v for k, v in marks.items()})
        et["index_sizes"] = ct.index_sizes("esci_et")
        out["esci_et"] = et
        f = RESULTS_DIR / "pinot_et_timings.json"
    else:
        job = ct.local_job(Path(a.job_log))
        loc = {"method": "standalone SegmentCreationAndMetadataPush job (3 threads) reading the 33 Parquet "
                         "files of esci.products_us_pi from MinIO; t0 = job start",
               "ingestion_job": job,
               "job_wall_s(t0 -> job exit)": round(a.job_end - a.t0, 1) if a.job_end else None,
               "per_segment_server_load(download+untar+open)": ct.local_server_load()}
        loc.update({"watch_" + k: v for k, v in marks.items()})
        loc["index_sizes"] = ct.index_sizes("esci_local")
        out["esci_local"] = loc
        f = RESULTS_DIR / "pinot_local_timings.json"
    f.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2)[:5000])


if __name__ == "__main__":
    main()
