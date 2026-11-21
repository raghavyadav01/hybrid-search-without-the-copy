# Hybrid Product Search Without the Copy: code and results

Artifact for *Hybrid Product Search Without the Copy* (R. Yadav, X. Fu, C. Soman), ISIR-eCom 2026 at IEEE BigData 2026.

This repository holds the evaluation scripts, engine configurations and raw results behind every number in the
paper. The data is not included; the scripts rebuild it from the public
[Amazon ESCI](https://github.com/amazon-science/esci-data) dataset.

## What you can reproduce

| Part | Needs |
|---|---|
| All tables and figures from the stored results (`results/`, `ec2/results/`) | Python 3.12, numpy, pyarrow, pandas |
| Reference runs (exact BM25, exact vector, RRF) | the above + `bm25s`, ESCI download, BGE embeddings (`embed.py`) |
| Elasticsearch 9.5.5 / OpenSearch 3.9.0 baselines | Docker (`docker-compose.yml`, `search_baselines.py`) |
| Pinot BM25 scoring (`TEXT_SCORE`, `TEXT_SCORE_STATS`) | Apache Pinot with PRs #20070–#20073 and #20075 |
| Pinot serving Iceberg files in place (external tables, S3 tier) | StarTree Pinot; not part of Apache Pinot |

## Map from paper to code

| Paper | Script | Results |
|---|---|---|
| Table I (quality) | `ref_*.py`, `search_baselines.py`, `pinot_runs.py`, `make_table1.py` | `results/table1.md`, `results/table1_significance.json`, `results/*per_query.parquet` |
| Matched-budget comparison (§5.1) | `pinot_runs.py --seg-k 30`, `analysis/budget_matched_sig.py` | `results/pinot_et_*_k30.*` |
| BM25 vs. reference (§5.1) | `analysis/bm25_vs_reference_sig.py` | `results/ref_bm25.*`, `results/pinot_et_bm25_global.*` |
| Ablations, 3,685 hurt queries (§5.2) | `analysis/hurt_queries_*.py` | `results/hurt_queries_global_vs_perfile.parquet` |
| Table II, 277M lake latency (§5.3) | `ec2/make_1tb.py`, `ec2/onboard_1tbb.sh`, `ec2/build_term_stats.py`, `mpbench.py` | `ec2/results/results_1tb_*.json` |
| Second replica (§5.3) | `ec2/provision_b.sh`, `ec2/bootstrap_b.sh`, `mpbench.py` | `results/ec2_replicas_rep{1,2}.json` |
| Engine latency at 1.2M (§5.3) | `brand_bench.py` | `ec2/results/ec2_brand_bench.json` |
| Table III, onboarding and storage (§5.4) | `collect_timings*.py`, `make_storage.py`, `make_timings.py` | `ec2/results/ec2_*_timings.json`, `results/storage.md` |
| Table IV, infrastructure cost (§5.5) | derived from Tables II–III and list prices | — |
| Figure 2, freshness (§5.6) | `freshness.py` | `ec2/results/ec2_freshness_*.json` |

## Running on AWS

`ec2/provision_b.sh` creates a tagged bucket, key pair, security group, IAM role and three instances
(two r7g.2xlarge Pinot nodes, one c7g.8xlarge load generator) in the account of your current AWS profile;
set `EXPECT_ACCOUNT` to guard against the wrong one and `OWNER` for the tags. `ec2/bootstrap_b.sh` uploads the lake
and sets up the boxes. Replace `YOUR-BUCKET` in the table configs with your bucket. Delete the instances and bucket
when done; nothing is torn down automatically.

## Notes

- Paths such as `$EVAL_HOME`, `$PINOT_HOME` and `$STARTREE_PINOT_HOME` stand for your checkout locations.
- Logs under `results/logs` and `ec2/results` are kept as produced; host names and account IDs were replaced.
- `ec2/results/*_timings.json` carry a fixed "on the laptop" label from the collection script; the files under
  `ec2/` were measured on EC2.

## License

Apache License 2.0. ESCI is distributed by Amazon under its own license.
