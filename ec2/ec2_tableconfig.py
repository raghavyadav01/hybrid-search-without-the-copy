"""Laptop table config -> EC2: real S3 (instance role, default credential chain) instead of MinIO; everything else
(indexes, HNSW buffer/threads, poll interval, tier settings) unchanged.  python3 ec2_tableconfig.py IN OUT BUCKET REGION"""
import json
import sys

src, dst, bucket, region = sys.argv[1:5]
d = json.load(open(src))
o = d["offline"]
sync = o["task"]["taskTypeConfigsMap"]["ExternalTableSyncTask"]
p = "catalog.iceberg-rest."
sync[p + "restUri"] = "http://localhost:8190"
sync[p + "warehouse"] = f"s3://{bucket}/warehouse/"
for k in ("accessKeyId", "secretAccessKey", "endpoint", "authType"):
    sync.pop(p + "auth.storage." + k, None)
sync[p + "auth.storage.region"] = region
tier = o["tierConfigs"][0]["tierBackendProperties"]
for k in ("endpoint", "enable.path.style.access", "accessKey", "secretKey"):
    tier.pop(k, None)
tier.update({"bucket": bucket, "region": region, "pathPrefix": "segments/esci_et"})
json.dump(d, open(dst, "w"), indent=2)
print("wrote", dst)
