# ISIR-eCom EC2 benchmark — 2026-10-26 (us-west-2)

## Setup
- Account: pinotteam (YOUR_AWS_ACCOUNT_ID), us-west-2a, one VPC/SG, instance role scoped to the bench bucket.
- System-under-test boxes (one per system, identical): **r7g.2xlarge** (Graviton3, 8 vCPU, 64 GiB), Amazon Linux 2023
  (ami-0e7e8cbdc1a628256, kernel 6.18), gp3 200 GiB / 6000 IOPS / 500 MB/s.
- Load generator: **c7g.2xlarge** (8 vCPU), same AZ; Python client, same 1,000 sampled queries + 200 warm-up, seed 2026.
- Lake: the laptop's 33 Parquet files of `esci.products_us_pi`, byte-identical, in S3
  `s3://YOUR-BUCKET/warehouse/esci/products_us_pi/` (format v2, identity(locale), zstd,
  64 KB pages, page index, 1 row group/file, 4.38 GB); Iceberg REST catalog (apache/iceberg-rest-fixture 1.9.1) on the
  Pinot box, files registered with pyiceberg add_files.
- Pinot: the laptop build (OSS 1.7.0-SNAPSHOT + TEXT_SCORE / TEXT_SCORE_STATS / VECTOR_DISTANCE, STP 2.0.0-SNAPSHOT
  with index-only prefetch skip), Corretto 25.0.5, one JVM (controller, broker, 3 servers, minion), -Xmx24g,
  MaxDirectMemory 24g, Lucene SIMD on (--add-modules jdk.incubator.vector + patched version check). External table over
  S3 (tier = S3 in the same bucket); HNSW M=16 beam 100 cosine, maxBufferSizeMB=512, parallel build (8 threads) for
  files ≤ 20k rows; sync readiness poll 500 ms.
- Elasticsearch 9.5.5 / OpenSearch 3.9.0: docker, single node, heap 16 GB in a 56 GB container (rest = page cache),
  1 shard 0 replicas, `_source` disabled, standard analyzer + English stop words, HNSW m=16 ef_construction=100 float32,
  bulk-loaded from the same S3 files (4 workers, 500 docs/request), refreshed, force-merged to 1 segment.

## Results
Quality on EC2 = laptop exactly (s2000): Pinot vector nDCG@10 .3472 / R@100 .6062, hybrid .3615 / .6233.

Onboarding (time to fully queryable, 1,215,854 products):
| System | Queryable | Notes |
|---|---|---|
| Pinot lake-native | **149 s** (first segment 31 s) | register + per-file text/HNSW build, reads S3 directly |
| Elasticsearch | 447 s (707 s incl. force-merge 260 s) | read S3 → bulk → refresh |
| OpenSearch | 435 s (667 s incl. force-merge 231 s) | same |

Freshness (10,000-row Iceberg commit → all searchable, median of 3; engines handed the rows at commit, no pipeline lag):
| System | Runs (s) | Median |
|---|---|---|
| Pinot lake-native | 4.86 / 3.80 / 3.77 | 3.80 s |
| Elasticsearch | 3.04 / 3.13 / 3.18 | 3.13 s |
| OpenSearch | 3.57 / 3.59 / 3.61 | 3.59 s |
Pinot breakdown (run 3): ~1.1 s commit→segment planned (Iceberg metadata from S3), text 0.46 s, HNSW 0.6 s,
synchronous tier upload of built indexes to S3 0.66 s, open 0.45 s, convergence poll ≤0.5 s.

Latency (client ms; p50/p99 at concurrency 1; QPS at 8 / 32):
| Mode | Pinot | Elasticsearch (nc 200) | OpenSearch (ef 200) |
|---|---|---|---|
| BM25 | 10.4/22.1, 178/200 (global stats) | 4.6/7.0, 936/931 | 4.1/6.2, 921/929 |
| Vector (k100/file) | 16.4/21.3, 112/116 | 4.9/6.0, 652/627 | 4.6/5.9, 631/615 |
| Hybrid RRF | 50.0/61.0, 47/47 | 6.1/8.5, 624/630 | 6.8/12.2, 602/616 |
| Filtered hybrid | 50.6/61.5, 46/45 | – | – |
Matched recall: Pinot k=20/file (R@100 .566) 11.3 ms, 217/238 QPS; k=50 (.599) 13.4 ms, 156/165 QPS;
ES nc 1000 (.591) 5.2 ms, 640/621 QPS; OS ef 1000 (.593) 6.7 ms, 601/619 QPS.
ES/OS QPS plateaus at c8≈c32: partly the single Python load generator.

Raw files: `ec2/results/` (ec2_pinot_et_latency*.json, es_/os_latency*.json, ec2_freshness_*.json, index_*.log).

## CORRECTION + full matrix (23:30 PDT) — supersedes the latency table above
The first latency pass used a threaded Python client on a c7g.2xlarge; it capped ES/OS at ~600 QPS (Little's law:
ES hybrid c32 p50 26 ms ⇒ ~1,200 QPS possible). Re-measured with `mpbench.py` (one process per client) on a
c7g.8xlarge (32 vCPU), conc 1/8/32/64, 1,000 queries after 200 warm-up. Raw: results/ec2_mp.json (ES/OS + Pinot before
the zero-copy index input) and results/ec2_mp_fix.json (Pinot after it, + local copy).

p50 at c1 (ms) / peak QPS:
| Mode | Pinot lake (33 files) | Pinot local copy | ES nc1000 | OS ef1000 |
|---|---|---|---|---|
| BM25 (global stats) | 12.6 / 185 | 7.5 / 281 | 3.2 / 3,336 | 3.1 / 3,379 |
| Vector k100/file | 14.4 / 148 | 12.7 / 181 | 5.0 / 1,995 | 6.5 / 1,399 |
| Vector, 8 files (512 MB) | 9.0 / 365 | – | – | – |
| Hybrid RRF | 51.1 / 47 | 42.9 / 64 | 6.2 / 1,283 | 9.0 / 962 |
| Brand vector (brand-sorted lake, 31/33 files pruned) | 8.5 / 771 | – | 2.6 / 4,957 | 2.4 / 5,643 |
| Brand hybrid | 36.7 / 126 | – | 3.2 / 3,039 | 2.8 / 3,686 |
ES/OS nc200: vector 3.8 / 3,059 and 3.9 / 2,847; hybrid 5.0 / 1,722 and 6.1 / 1,438.
Quality on EC2: lake = local (vector .3472, hybrid .3615/.3615).
Local copy on EC2: ingestion job 308.5 s, queryable 311.6 s (lake-native 149 s).
Takeaways: lake-native = 66–82% of local-copy throughput; ES/OS ≥10× single-node throughput. External tables require
RAW columns, so no inverted index on brand (the filter scans the column in unpruned files).

## 1 TB run (2026-10-27/28) — 277M products, one i4i.4xlarge (16 vCPU, 128 GB, NVMe), indexes mmap'd
Lake: esci.products_1tb_brand, 228 copies of ESCI (copy 0 = original; others new ids + embedding noise sigma 0.012),
sorted by brand, 2,052 files, 806 GB. Onboarding 2 h 44 min, indexes 1.1 TB. Load generator r7g.16xlarge (mpbench).
Fixes made tonight (both measured, both keep results identical):
1. Prune segments whose filter column is entirely null (numNulls == totalDocs): brand query 123 -> 1 segment searched.
   OSS commit "Prune segments where the filtered column is entirely null" (+ test), branch hybrid-search-paper.
2. Corpus statistics materialized per snapshot for the whole vocabulary (ec2/build_term_stats.py: 658,670 terms,
   1,167 s); mpbench --term-stats; 97.6% of queries need no stats request; rankings identical on 179/179 checked.
Results (brand-scoped, 1,000 queries; 32-client rows = median of 20-40 passes after warm-up):
| Mode | 1 client p50/p99 ms | QPS @32 | p50/p99 @32 ms |
| vector | 7.5/9.5 | 1,103 (40 passes, min 738, max 1,143) | 27/53 |
| BM25 (term stats) | 6.5/47 | 635 | 40/176 |
| hybrid, client RRF | 8.9/48 | 375 | 75/246 |
| hybrid, SQL RRF (MSE) | 33/74 | 122 | 252/389 |
| vector, no filter | 5,146/6,150 | 0.2 | -- |
Before fixes: vector/BM25/hybrid peak 30/24/17 QPS. After null pruning only: 760/60/40.
Working set: same load over 21k queries/brands -> 318 vector QPS; first touch of a brand p50 237 ms.
Freshness on the 1 TB lake (10k-row commit -> searchable): 4.88 / 4.26 / 3.73 s, median 4.3 s.
Raw: results/results_1tb_*.json|log, results/freshness_1tb.log, results/term_stats_1tb.json.
Statistics cost (stime.py, 60 brand queries, idle node, after null pruning): live TEXT_SCORE_STATS over 2,052
segments p50 25.9 ms (server 24.5 ms); scored BM25 with those statistics on 1 segment p50 6.5 ms (server 5.0 ms).
Hybrid breakdown (htime.py, idle node): SQL RRF p50 33.0 ms vs BM25 6.3 ms + vector 8.1 ms single-stage.
Vocabulary table: 658,669 terms (658,670 tokens, one without stats), 1,167 s.
Registration of the 33-file table on EC2 (results/ec2_pinot_quickstart.log): sync start 03:57:28.116, all 33
segments uploaded/registered 03:57:31.561 -> 3.4 s.
