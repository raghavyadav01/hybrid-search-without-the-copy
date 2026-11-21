import sys, re, numpy as np, pandas as pd, pyarrow.parquet as pq
R, D = sys.argv[1], sys.argv[2]
def load(n): return pq.read_table(f'{R}/{n}.per_query.parquet').to_pandas().set_index('query_id')['ndcg_at_10']
g, p = load('pinot_et_bm25_global'), load('pinot_et_bm25')
q = pq.read_table(f'{D}/qrels_test_us.parquet').to_pandas()
print(q.columns.tolist())
qt = q.drop_duplicates('query_id').set_index('query_id')['query']
qt.index = qt.index.astype(str); g.index=g.index.astype(str); p.index=p.index.astype(str)
df = pd.DataFrame({'g':g,'p':p}).join(qt)
df['d']=df.g-df.p
df['cls']=np.where(df.d>0,'win',np.where(df.d<0,'lose','tie'))
toks=lambda s: re.findall(r'\w+', s.lower())
df['ntok']=df['query'].map(lambda s: len(toks(s)))
df['has_digit']=df['query'].str.contains(r'\d')
df['has_1char']=df['query'].map(lambda s: any(len(t)==1 for t in toks(s)))
# corpus df of terms -> rarity
print(df.groupby('cls').agg(n=('d','size'), ntok=('ntok','mean'), digit=('has_digit','mean'), onechar=('has_1char','mean'), g=('g','mean'), p=('p','mean'), d=('d','mean')).round(3))
print(df.groupby(pd.cut(df.ntok,[0,1,2,3,5,100]))['cls'].value_counts(normalize=True).unstack().round(3))
L=df[df.cls=='lose']; print('lose mean drop', L.d.mean().round(3), 'median', L.d.median().round(3), 'share drop<0.05', (L.d>-0.05).mean().round(3))
print(L.sort_values('d').head(15)[['query','p','g']].to_string())
df.to_parquet('results/hurt_queries_profile.parquet')
