P=$1; cd ~/eval
for k in 100 50; do
  for m in vector hybrid_sql; do python3 pinot_runs.py --table big --mode $m --sample 2000 --vector-fn index --seg-k $k --broker http://$P:18000 2>&1 | grep '"ndcg@10"' | sed "s/^/k$k $m: /"; done
  python3 pinot_runs.py --table big --latency --vector-fn index --seg-k $k --broker http://$P:18000 --lat-conc 1,8,32 --lat-modes bm25_global,vector,hybrid_sql > /dev/null 2>&1
  cp results/pinot_big_latency.json results/ec2_pinot_big_latency.k$k.json
done
echo BIG-DONE
