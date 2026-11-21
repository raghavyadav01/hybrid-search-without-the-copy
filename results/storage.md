# Storage — bytes per system (ESCI US, 1,215,854 products, 768-d float32 vectors)

| System | Text index | Vector index | Row data (forward / `_source`) | Other | Total | Extra bytes beyond the lake |
|---|---|---|---|---|---|---|
| Lake: Iceberg Parquet `esci.products_us_pi` (33 files, zstd) | - | - | 4.38 GB | - | 4.38 GB | 0.00 GB |
| Pinot External Table (server-side: indexes only; data read in place) | 0.27 GB | 3.78 GB | 0.00 GB | 0.00 GB | 4.06 GB | 4.06 GB |
| Pinot local copy (segments: forward + indexes) | 0.27 GB | 3.78 GB | 4.92 GB | 0.09 GB | 9.07 GB | 9.07 GB |
| Elasticsearch 9.5.5 (1 shard, force-merged, `_source` disabled) | 0.24 GB | 3.78 GB | 0.00 GB | 0.07 GB | 4.10 GB | 4.10 GB |
| OpenSearch 3.9.0 (1 shard, force-merged, `_source` disabled) | 0.50 GB | 3.78 GB | 0.00 GB | 0.00 GB | 4.29 GB | 4.29 GB |

- The lake is shared: every system reads these Parquet files; only Pinot ET serves queries from them in place, so its extra bytes are just the text + HNSW indexes (+ null vectors, metadata).
- Pinot local: per-index bytes summed over all segments from the servers' segment-metadata API (forward index = raw/dictionary columns incl. the vectors).
- ES: per-field bytes from `_disk_usage` (text = the `text` field, vector = `embedding`); OS: by Lucene file extension (vec/vex/vem* = vector, rest = text postings, norms, ids, keywords). `_source` is disabled in both (a production deployment that keeps `_source` stores the row data again: add about the size of the JSON documents). 'Other' = store minus text minus vector.
- GB = 10^9 bytes.
