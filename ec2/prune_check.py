import json, sys, requests
t = sys.argv[1]
top = requests.post("http://localhost:18000/query/sql", json={"sql": f"select brand, count(*) c from {t} where brand is not null group by brand order by c desc limit 5", "queryOptions": "enableNullHandling=true"}).json()
print("top brands", top.get("resultTable", {}).get("rows"), top.get("exceptions"))
for b in [r[0] for r in top["resultTable"]["rows"][:3]] + ["Sony"]:
    d = requests.post("http://localhost:18000/query/sql", json={"sql": f"select count(*) from {t} where brand = '{b}'"}).json()
    print(b, d.get("resultTable", {}).get("rows"), {k: d.get(k) for k in ["numSegmentsQueried", "numSegmentsProcessed",
          "numSegmentsMatched", "numSegmentsPrunedByServer", "numSegmentsPrunedByValue", "timeUsedMs"]}, d.get("exceptions"))
