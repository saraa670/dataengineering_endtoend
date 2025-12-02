# extract_api_to_minio.py
import requests
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from minio import Minio
from datetime import date
import io

# ----------------------------------------------------
# Load configuration
# ----------------------------------------------------
with open("config/config.json") as f:
    cfg = json.load(f)

minio_cfg = cfg["minio"]
api_cfg = cfg["api"]
bucket = minio_cfg["bucket"]
landing_prefix = cfg["paths"]["landing_prefix"]
today = date.today().isoformat()

# ----------------------------------------------------
# MinIO Client
# ----------------------------------------------------
client = Minio(
    minio_cfg["endpoint"].replace("http://","").replace("https://",""),
    access_key=minio_cfg["access_key"],
    secret_key=minio_cfg["secret_key"],
    secure=False  # using local MinIO (http)
)

# Ensure bucket exists
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Created bucket: {bucket}")
else:
    print(f"Bucket exists: {bucket}")

# ----------------------------------------------------
# API endpoints (collection endpoints, no {id})
# ----------------------------------------------------
endpoints = {
    
    "businessEntityAddresses": "adventureworks/api/v1/businessEntityAddresses",
    "addressId": "adventureworks/api/v1/addressId",
    "province": "adventureworks/api/v1/province",
    "ordersShippedTo": "adventureworks/api/v1/ordersShippedTo"
}

# ----------------------------------------------------
# Fetch from API and upload to MinIO
# ----------------------------------------------------
for name, path in endpoints.items():
    url = api_cfg["base_url"].rstrip("/") + "/" + path
    print(f"\nRequesting {name}: {url}")

    try:
        r = requests.get(url, timeout=api_cfg.get("timeout", 60))
        r.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        continue

    payload = r.json()
    print(f"Payload type for {name}: {type(payload)}")

    # Extract list of records
    arr = []
    if isinstance(payload, dict):
        # API returns {"data": [...]}, which we need
        if "data" in payload and isinstance(payload["data"], list):
            arr = payload["data"]
        else:
            # fallback: find first list in values
            arr = next((v for v in payload.values() if isinstance(v, list)), [])
    elif isinstance(payload, list):
        arr = payload

    if not arr:
        print(f"No data returned for {name}")
        continue

    df = pd.DataFrame(arr)
    print(f"{name}: {len(df)} rows fetched")

    # Convert to Parquet bytes
    table = pa.Table.from_pandas(df)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    data_bytes = buf.getvalue().to_pybytes()

    # Debug: optional local save
    
    # MinIO object path
key = f"{landing_prefix}/api_{name}/{today}/data.parquet"
print(f"Uploading {len(data_bytes)} bytes to {bucket}/{key}")

# Upload to MinIO
try:
    client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=io.BytesIO(data_bytes),
        length=len(data_bytes),
        content_type="application/parquet"
    )
    print(f"Uploaded {key}, rows: {len(df)}")
except Exception as e:
    print(f"Failed to upload {key}: {e}")
