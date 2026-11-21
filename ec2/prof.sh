cd ~/eval
PID=$(pgrep -f '[S]3ParquetQuickstart')
python3 - <<'PY' &
import sys, time, requests
sys.path.insert(0, '.')
import pinot_runs as pr
pr.VECTOR_FN = "index"
v = pr.load_query_vectors(); qs = list(v)[:400]
from concurrent.futures import ThreadPoolExecutor
def one(q): requests.post("http://localhost:18000/query/sql", json={"sql": pr.vector_sql("esci_et", v[q])})
t = time.time()
with ThreadPoolExecutor(16) as ex:
    while time.time() - t < 40: list(ex.map(one, qs[:64]))
PY
sleep 8
for i in $(seq 25); do /usr/lib/jvm/java-25-amazon-corretto/bin/jcmd $PID Thread.print 2>/dev/null; sleep 0.4; done > /tmp/stacks.txt
wait
python3 - <<'PY'
import re, collections
txt = open('/tmp/stacks.txt').read().split('\n\n')
c = collections.Counter(); tot = 0
for blk in txt:
    if 'RUNNABLE' not in blk or ('pqr-' not in blk and 'query' not in blk.lower() and 'pool' not in blk): continue
    frames = [l.strip()[3:] for l in blk.split('\n') if l.strip().startswith('at ')]
    if not frames: continue
    tot += 1
    top = next((f for f in frames if 'org.apache' in f or 'ai.startree' in f), frames[0])
    c[re.sub(r'\(.*', '', top)] += 1
print('runnable samples', tot)
for k, v in c.most_common(25): print(v, k)
PY
