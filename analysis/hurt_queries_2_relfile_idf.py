import re, math, numpy as np, pandas as pd, pyarrow.parquet as pq
from collections import Counter, defaultdict
S='results'
STOP=set("a an and are as at be but by for if in into is it no not of on or such that the their then there these they this to was will with".split())
tok=lambda s:[t for t in re.findall(r'\w+',s.lower()) if t not in STOP]
df=pd.read_parquet(f'{S}/hurt.parquet')
qterms={qid:set(tok(s)) for qid,s in df['query'].items()}
V=set().union(*qterms.values())
t=pq.read_table('data/products_us.parquet',columns=['product_id','text'])
ids=t.column('product_id').to_pylist(); texts=t.column('text').to_pylist(); N=len(ids); NF=33
fid=lambda i:i*NF//N
dff=[Counter() for _ in range(NF)]; nf=Counter()
for i,s in enumerate(texts):
    f=fid(i); nf[f]+=1
    for w in set(tok(s)) & V: dff[f][w]+=1
dfg=Counter()
for c in dff: dfg.update(c)
idf=lambda n,N:math.log(1+(N-n+0.5)/(n+0.5))
pos={pid:fid(i) for i,pid in enumerate(ids)}
q=pq.read_table('data/qrels_test_us.parquet').to_pandas(); q=q[q.esci_label=='E']
relf=q.assign(f=q.product_id.map(pos)).groupby('query_id').f.agg(lambda s:s.value_counts().index[0]); relf.index=relf.index.astype(str)
rows=[]
for qid,terms in qterms.items():
    if qid not in relf.index or not terms: rows.append(np.nan); continue
    f=relf[qid]; terms=[w for w in terms if dfg[w]>0]
    if not terms: rows.append(np.nan); continue
    # boost of relevant file: mean over terms of (local idf in rel file - mean local idf over files where term occurs)
    b=[]
    for w in terms:
        loc=[idf(dff[k][w],nf[k]) for k in range(NF) if dff[k][w]>0]
        if dff[f][w]>0: b.append(idf(dff[f][w],nf[f]) - np.mean(loc))
    rows.append(np.mean(b) if b else np.nan)
df['relfile_idf_boost']=rows
print(df.groupby('cls').relfile_idf_boost.describe().round(3))
df['boosted']=df.relfile_idf_boost>0.25
print(pd.crosstab(df.boosted,df.cls,normalize='index').round(3)); print('boosted share',df.boosted.mean().round(3))
print('share of losses that are boosted', df[df.cls=='lose'].boosted.mean().round(3), 'vs wins', df[df.cls=='win'].boosted.mean().round(3))
df.to_parquet(f'{S}/hurt3.parquet')
