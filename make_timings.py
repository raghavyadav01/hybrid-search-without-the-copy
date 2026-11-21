"""Builds results/timings.md: ingestion, index build, time to queryable, index sizes and the isolated
latency pass (c=1 and c=8, 1,000 queries) for every system of the sequential run.

  .venv/bin/python -I make_timings.py
"""
import json
import platform
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I drops the script dir

from common import RESULTS_DIR


def load(n):
    f = RESULTS_DIR / f"{n}.json"
    return json.loads(f.read_text()) if f.exists() else None


def gib(b):
    return f"{b / 2**30:.2f} GiB" if b else "-"


def mib(b):
    return f"{b / 2**20:.0f} MiB" if b else "-"


def s(x):
    return "-" if x is None else f"{x:,.0f} s" if x >= 100 else f"{x:.1f} s"


def hw():
    def sc(k):
        return subprocess.run(["sysctl", "-n", k], capture_output=True, text=True).stdout.strip()
    return (f"{sc('machdep.cpu.brand_string')}, {sc('hw.ncpu')} cores, {int(sc('hw.memsize')) / 2**30:.0f} GB RAM, "
            f"macOS {platform.mac_ver()[0]}; Docker Desktop VM for MinIO / Iceberg REST / ES / OS")


def main():
    out = ["# Timings — ingestion, indexing, time to queryable, size, latency", ""]
    out.append(f"Hardware: {hw()}.")
    out.append("")
    out.append("Each system ran **alone**: the Pinot External Table cluster, then a fresh Pinot cluster for the "
               "local copy, then Elasticsearch, then OpenSearch; other Docker stacks were paused and no other "
               "benchmark or build was running. MinIO + the Iceberg REST catalog (the lake) stayed up for all. "
               "All systems read the same Iceberg table `esci.products_us_pi` (1,215,854 rows, 33 Parquet "
               "files).")
    out.append("")
    ice = load("iceberg_esci_products_us_pi")
    if ice:
        emb = ice["columns"].get("embedding.list.element", {})
        txt = ice["columns"].get("text", {})
        out.append("## Lake table")
        out.append("")
        out.append(f"- `esci.products_us_pi`: {ice['data_files']} files, {ice['file_mb_min']}–{ice['file_mb_max']} MB "
                   f"(median {ice['file_mb_median']}), {ice['total_gb']} GB, zstd, 1 row group per file, page index "
                   f"(ColumnIndex + OffsetIndex) on every column chunk: {ice['page_index_on_every_column_chunk']}.")
        out.append(f"- Pages: embedding {emb.get('data_pages'):,} data pages (p50 {emb.get('data_page_compressed_bytes_p50'):,} B), "
                   f"text {txt.get('data_pages'):,} (p50 {txt.get('data_page_compressed_bytes_p50'):,} B); "
                   f"write {ice['write_secs']} s with pyiceberg/pyarrow (+{ice['input_load_secs']} s to load the "
                   "inputs; written while a Maven build ran, so indicative).")
        out.append("")

    out.append("## Ingestion, index build, time to queryable, size")
    out.append("")
    out.append("| System | Ingestion / registration | Index build | Time to fully queryable | Text index | Vector index | Other / total |")
    out.append("|---|---|---|---|---|---|---|")
    pe = load("pinot_et_timings")
    if pe:
        e = pe["esci_et"]
        b = e["index_sizes"]["bytes_by_index_type"]
        out.append(
            f"| Pinot External Table | none (data stays in Iceberg); catalog sync + segment registration "
            f"{s(e['catalog_sync_registration_s'])}, all 33 segments in IdealState at {s(e['watch_all_segments_in_idealstate_s'])} "
            f"| at server load, per segment: text {e['per_segment_text_index_build']['p50_s']} s p50 "
            f"(sum {s(e['per_segment_text_index_build']['sum_s'])}), HNSW {e['per_segment_hnsw_build']['p50_s']} s p50 "
            f"(sum {s(e['per_segment_hnsw_build']['sum_s'])}); wall (first load → last loaded) "
            f"{s(e['index_build_wall_s(first load start -> last segment loaded)'])} "
            f"| {s(e['watch_all_segments_online_s(time to fully queryable)'])} (first segment queryable at "
            f"{s(e['watch_first_segment_online_s'])}) | {mib(b.get('text_index'))} | {gib(b.get('vector_index'))} "
            f"| null vectors {mib(b.get('nullvalue_vector'))}; forward data = the Parquet files (0 B in Pinot) |")
    pl = load("pinot_local_timings")
    if pl:
        e = pl["esci_local"]
        j = e["ingestion_job"]
        b = e["index_sizes"]["bytes_by_index_type"]
        other = sum(v for k, v in b.items() if k not in ("text_index", "vector_index"))
        out.append(
            f"| Pinot local copy | segment creation + metadata push job {s(e.get('job_wall_s(t0 -> job exit)'))} "
            f"(3 threads, {j['segments']} segments; per segment create p50 {j['per_segment_total_create']['p50_s']} s) "
            f"| inside segment creation, per segment p50: record indexing incl. HNSW insert "
            f"{(j['per_segment_record_indexing_incl_hnsw_insert_and_text_docs'] or {}).get('p50_s')} s, HNSW seal/merge "
            f"{(j['per_segment_hnsw_seal_merge'] or {}).get('p50_s')} s, text finish "
            f"{(j['per_segment_text_index_finish'] or {}).get('p50_s')} s "
            f"| {s(e['watch_all_segments_online_s(time to fully queryable)'])} from job start "
            f"| {mib(b.get('text_index'))} | {gib(b.get('vector_index'))} | forward + other {gib(other)}; "
            f"total {gib(sum(b.values()))} |")
    for eng, name in (("es", "Elasticsearch"), ("os", "OpenSearch")):
        t = load(f"{eng}_timings")
        if not t:
            continue
        ig, ix, sz = t["ingestion"], t["indexing"], t["index_size"]
        tb = sz.get("es_disk_usage_text_bytes")
        vb = sz.get("es_disk_usage_embedding_bytes")
        if vb is None and sz.get("split"):
            vals = list(sz["split"].values())
            vb, tb = vals[0], vals[1]
        sv = t.get("server_stats") or {}
        out.append(
            f"| {name} {t['version']} | bulk load {s(ig['bulk_wall_secs'])} (4 client processes, 500 docs/request, "
            f"reading the 33 Parquet files) | inside bulk + refresh {s(ix['final_refresh_secs'])} + merges settle "
            f"{s(ix['merge_settle_secs'])}; engine stats (thread-summed): indexing {sv.get('indexing_index_time_secs', '-')} s, "
            f"merges {sv.get('merges_total_time_secs', '-')} s "
            f"| {s(ix['total_until_searchable_and_merges_settled_secs'])} | {gib(tb)} | {gib(vb)} | store {gib(sz['store_bytes'])} |")
    out.append("")
    out.append("Pinot sizes are summed over all segments from the servers' segment-metadata API; ES text/vector "
               "from `_disk_usage`, OS from segment file extensions (vec/vex/vem* = vector). ES/OS store "
               "`_source` disabled. HNSW build time is not separable in ES/OS stats.")
    out.append("")

    out.append("## Latency (isolated pass: same 1,000 queries, after 200 warm-up queries)")
    out.append("")
    out.append("Client wall time per query (ms) and throughput, concurrency 1 and 8. Pinot global-stats modes "
               "include both requests (TEXT_SCORE_STATS + query); client-side RRF = the BM25 then kNN request.")
    out.append("")
    out.append("| System | Mode | c=1 p50 | p95 | p99 | QPS | c=8 p50 | p95 | p99 | QPS |")
    out.append("|---|---|---|---|---|---|---|---|---|---|")
    labels = {"bm25": "BM25", "bm25_global": "BM25 global stats", "vector": "Vector HNSW", "bm25_perseg_note": "",
              "hybrid_sql": "Hybrid RRF SQL (global BM25)", "hybrid_filter": "Hybrid RRF SQL + filter",
              "knn": "kNN HNSW", "hybrid_native": "Hybrid RRF native", "hybrid_client": "Hybrid RRF client"}
    for f, name in (("pinot_et_latency", "Pinot External Table"), ("pinot_local_latency", "Pinot local copy"),
                    ("es_latency", "Elasticsearch"), ("es_latency_c1000", "Elasticsearch (num_candidates=1000)"), ("os_latency", "OpenSearch"), ("os_latency_c1000", "OpenSearch (ef_search=1000)")):
        d = load(f)
        if not d:
            continue
        for m, v in d.items():
            if m == "config":
                continue
            cells = []
            for c in ("c1", "c8"):
                x = v.get(c)
                cells += ([f"{x['client_ms_p50']:.0f}", f"{x['client_ms_p95']:.0f}", f"{x['client_ms_p99']:.0f}",
                           f"{x['qps']:.1f}"] if x else ["-"] * 4)
            err = sum((v.get(c) or {}).get("errors", 0) for c in ("c1", "c8"))
            out.append(f"| {name} | {labels.get(m, m)}{f' ({err} errors)' if err else ''} | " + " | ".join(cells) + " |")
    out.append("")
    cold = load("pinot_et_cold")
    if cold:
        out.append("Pinot External Table cold first query after a cluster restart (query "
                   f"`{cold['query']}`): " + ", ".join(f"{labels.get(m, m)} {v['client_ms']:.0f} ms"
                                                        for m, v in cold.items() if isinstance(v, dict)) + ".")
        out.append("")
    (RESULTS_DIR / "timings.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
