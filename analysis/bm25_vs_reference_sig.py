import sys, numpy as np, pyarrow.parquet as pq
R=sys.argv[1]
def load(n): t=pq.read_table(f'{R}/{n}.per_query.parquet').to_pandas(); return t.set_index('query_id')['ndcg_at_10']
a=load('pinot_et_bm25_global'); b=load('ref_bm25'); a,b=a.align(b,join='inner')
d=(a-b).values; print(len(d), a.mean(), b.mean(), d.mean(), (d>0).sum(), (d<0).sum())
rng=np.random.default_rng(2026); obs=abs(d.mean()); c=0
for _ in range(10000):
    s=rng.choice([-1,1],len(d)); c+= abs((d*s).mean())>=obs
print('p', (c+1)/10001)
g=load('pinot_et_bm25_global'); p=load('pinot_et_bm25'); g,p=g.align(p,join='inner'); dd=g-p
print('global vs perseg win/lose', (dd>0).sum(), (dd<0).sum())
