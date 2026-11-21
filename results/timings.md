# Timings — ingestion, indexing, time to queryable, size, latency

Hardware: Apple M2 Max, 12 cores, 32 GB RAM, macOS 14.5; Docker Desktop VM for MinIO / Iceberg REST / ES / OS.

Each system ran **alone**: the Pinot External Table cluster, then a fresh Pinot cluster for the local copy, then Elasticsearch, then OpenSearch; other Docker stacks were paused and no other benchmark or build was running. MinIO + the Iceberg REST catalog (the lake) stayed up for all. All systems read the same Iceberg table `esci.products_us_pi` (1,215,854 rows, 33 Parquet files).

## Lake table

- `esci.products_us_pi`: 33 files, 123.7–130.5 MB (median 126.4), 4.08 GB, zstd, 1 row group per file, page index (ColumnIndex + OffsetIndex) on every column chunk: True.
- Pages: embedding 55,281 data pages (p50 67,706 B), text 9,102 (p50 92,255 B); write 129.6 s with pyiceberg/pyarrow (+32.2 s to load the inputs; written while a Maven build ran, so indicative).

## Ingestion, index build, time to queryable, size

| System | Ingestion / registration | Index build | Time to fully queryable | Text index | Vector index | Other / total |
|---|---|---|---|---|---|---|
| Pinot External Table | none (data stays in Iceberg); catalog sync + segment registration 8.9 s, all 33 segments in IdealState at 13.4 s | at server load, per segment: text 3.1 s p50 (sum 110 s), HNSW 68.8 s p50 (sum 2,255 s); wall (first load → last loaded) 313 s | 328 s (first segment queryable at 93.5 s) | 262 MiB | 3.52 GiB | null vectors 1 MiB; forward data = the Parquet files (0 B in Pinot) |
| Pinot local copy | segment creation + metadata push job 762 s (3 threads, 33 segments; per segment create p50 58.1 s) | inside segment creation, per segment p50: record indexing incl. HNSW insert 20.6 s, HNSW seal/merge 29.8 s, text finish 0.2 s | 780 s from job start | 262 MiB | 3.52 GiB | forward + other 4.67 GiB; total 8.45 GiB |
| Elasticsearch 9.5.5 | bulk load 456 s (4 client processes, 500 docs/request, reading the 33 Parquet files) | inside bulk + refresh 0.1 s + merges settle 120 s; engine stats (thread-summed): indexing 0.0 s, merges 0.0 s | 577 s | 0.23 GiB | 3.52 GiB | store 3.82 GiB |
| OpenSearch 3.9.0 | bulk load 529 s (4 client processes, 500 docs/request, reading the 33 Parquet files) | inside bulk + refresh 0.6 s + merges settle 221 s; engine stats (thread-summed): indexing 0.0 s, merges 0.0 s | 751 s | 0.47 GiB | 3.52 GiB | store 3.99 GiB |

Pinot sizes are summed over all segments from the servers' segment-metadata API; ES text/vector from `_disk_usage`, OS from segment file extensions (vec/vex/vem* = vector). ES/OS store `_source` disabled. HNSW build time is not separable in ES/OS stats.

## Latency (isolated pass: same 1,000 queries, after 200 warm-up queries)

Client wall time per query (ms) and throughput, concurrency 1 and 8. Pinot global-stats modes include both requests (TEXT_SCORE_STATS + query); client-side RRF = the BM25 then kNN request.

| System | Mode | c=1 p50 | p95 | p99 | QPS | c=8 p50 | p95 | p99 | QPS |
|---|---|---|---|---|---|---|---|---|---|
| Pinot External Table | BM25 | 162 | 284 | 351 | 6.4 | 645 | 2478 | 3048 | 8.2 |
| Pinot External Table | BM25 global stats | 160 | 276 | 307 | 6.5 | 707 | 2561 | 3906 | 7.7 |
| Pinot External Table | Vector HNSW | 95 | 304 | 394 | 7.1 | 963 | 1496 | 1807 | 8.1 |
| Pinot External Table | Hybrid RRF SQL (global BM25) | 659 | 980 | 1247 | 1.5 | 3478 | 4745 | 5272 | 2.4 |
| Pinot External Table | Hybrid RRF SQL + filter | 614 | 850 | 964 | 1.6 | 3915 | 5446 | 6241 | 2.1 |
| Pinot local copy | BM25 | 10 | 22 | 40 | 50.2 | 25 | 55 | 80 | 289.1 |
| Pinot local copy | BM25 global stats | 19 | 28 | 38 | 50.7 | 41 | 58 | 67 | 190.6 |
| Pinot local copy | Vector HNSW | 402 | 697 | 1044 | 2.2 | 2047 | 3321 | 5379 | 3.6 |
| Pinot local copy | Hybrid RRF SQL (global BM25) | 1649 | 2463 | 4956 | 0.6 | 7787 | 11916 | 13900 | 1.0 |
| Pinot local copy | Hybrid RRF SQL + filter | 1575 | 2400 | 4776 | 0.6 | 4411 | 9974 | 13291 | 1.7 |
| Elasticsearch | BM25 | 3 | 5 | 8 | 331.3 | 6 | 12 | 16 | 1121.4 |
| Elasticsearch | kNN HNSW | 4 | 7 | 10 | 208.8 | 10 | 15 | 18 | 739.3 |
| Elasticsearch | Hybrid RRF native | 5 | 6 | 7 | 200.2 | 12 | 19 | 24 | 628.4 |
| Elasticsearch | Hybrid RRF client | 7 | 10 | 13 | 132.9 | 18 | 35 | 69 | 396.4 |
| Elasticsearch (num_candidates=1000) | kNN HNSW | 6 | 8 | 11 | 165.6 | 12 | 18 | 24 | 629.8 |
| Elasticsearch (num_candidates=1000) | Hybrid RRF native | 7 | 12 | 17 | 129.4 | 16 | 29 | 67 | 427.8 |
| Elasticsearch (num_candidates=1000) | Hybrid RRF client | 10 | 18 | 30 | 88.2 | 19 | 33 | 54 | 378.3 |
| OpenSearch | BM25 | 3 | 4 | 6 | 330.9 | 6 | 10 | 18 | 1140.8 |
| OpenSearch | kNN HNSW | 4 | 6 | 8 | 227.5 | 9 | 14 | 17 | 810.8 |
| OpenSearch | Hybrid RRF native | 7 | 16 | 27 | 126.2 | 13 | 22 | 27 | 569.4 |
| OpenSearch | Hybrid RRF client | 7 | 10 | 12 | 136.3 | 17 | 26 | 30 | 450.5 |
| OpenSearch (ef_search=1000) | kNN HNSW | 7 | 10 | 13 | 136.7 | 15 | 25 | 31 | 498.4 |
| OpenSearch (ef_search=1000) | Hybrid RRF native | 9 | 17 | 35 | 94.5 | 15 | 24 | 36 | 483.9 |
| OpenSearch (ef_search=1000) | Hybrid RRF client | 11 | 17 | 24 | 85.9 | 21 | 31 | 39 | 366.4 |

Pinot External Table cold first query after a cluster restart (query `bassinet on wheels`): BM25 3014 ms, BM25 global stats 67 ms, Vector HNSW 27804 ms, Hybrid RRF SQL (global BM25) 1550 ms, Hybrid RRF SQL + filter 934 ms.

