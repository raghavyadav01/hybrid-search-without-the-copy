# Budget-matched comparison: Pinot k=30 per file (33 files = 990 candidates) vs engines at 1,000 candidates.
import sys, numpy as np, pyarrow.parquet as pq
R = sys.argv[1] if len(sys.argv) > 1 else 'results'
def load(n): return pq.read_table(f'{R}/{n}.per_query.parquet').to_pandas().set_index('query_id')['ndcg_at_10']
def test(a_name, b_name):
    a, b = load(a_name).align(load(b_name), join='inner'); d = (a - b).values
    rng = np.random.default_rng(2026); obs = abs(d.mean())
    c = sum(abs((d * rng.choice([-1, 1], len(d))).mean()) >= obs for _ in range(10000))
    print(f'{a_name} vs {b_name}: n={len(d)} {a.mean():.4f} vs {b.mean():.4f} diff={d.mean():+.4f} p={(c+1)/10001:.4f}')
for p, e in [('pinot_et_vector_k30', 'es_knn_c1000'), ('pinot_et_vector_k30', 'os_knn_c1000'),
             ('pinot_et_hybrid_sql_k30', 'es_hybrid_native_c1000'), ('pinot_et_hybrid_sql_k30', 'os_hybrid_native_c1000'),
             ('pinot_et_hybrid_sql_k30', 'pinot_et_hybrid_sql')]:
    test(p, e)
