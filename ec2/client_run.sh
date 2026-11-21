#!/bin/bash
# Runs ON the client box. Args: PINOT_PRIV ES_PRIV OS_PRIV. Same queries/sample/seed as the laptop runs.
P=$1 ES=$2 OS=$3
cd ~/eval; mkdir -p results data/runs
L=results/ec2_run_log.md
echo "- $(date -u '+%F %T') start" >> $L
python3 pinot_runs.py --table et --latency --vector-fn index --broker http://$P:18000 --lat-conc 1,8,32 \
  --lat-modes bm25,bm25_global,vector,hybrid_sql,hybrid_filter > pinot_lat.log 2>&1
cp results/pinot_et_latency.json results/ec2_pinot_et_latency.json
for k in 20 50; do
  python3 pinot_runs.py --table et --latency --vector-fn index --seg-k $k --broker http://$P:18000 --lat-conc 1,8,32 --lat-modes vector >> pinot_lat.log 2>&1
  cp results/pinot_et_latency.json results/ec2_pinot_et_latency.k$k.json
done
echo "- $(date -u '+%F %T') pinot done" >> $L
for nc in 200 1000; do
  ESCI_SEARCH_HOST=$ES python3 search_baselines.py --engine es latency --num-cand $nc --lat-conc 1,8,32 --lat-modes bm25,knn,hybrid_native > es_lat_$nc.log 2>&1
  ESCI_SEARCH_HOST=$OS python3 search_baselines.py --engine os latency --num-cand $nc --lat-conc 1,8,32 --lat-modes bm25,knn,hybrid_native > os_lat_$nc.log 2>&1
done
echo "- $(date -u '+%F %T') ALL-DONE" >> $L
