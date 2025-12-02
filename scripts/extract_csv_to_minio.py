import os
import json
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from minio import Minio

# ----------------------------------------------------
# Load configuration
# ----------------------------------------------------
with open("config/config.json") as f:
    cfg = json.load(f)

minio_cfg = cfg["minio"]
raw_folder = cfg["paths"]["local_raw_folder"]
landing_prefix = cfg["paths"]["landing_prefix"]
bucket = minio_cfg["bucket"]

today = date.today().isoformat()

# ----------------------------------------------------
# MinIO Client
# ----------------------------------------------------
endpoint_cleaned = (
    minio_cfg["endpoint"]
    .replace("http://", "")
    .replace("https://", "")
)

client = Minio(
    endpoint_cleaned,
    access_key=minio_cfg["access_key"],
    secret_key=minio_cfg["secret_key"],
    secure=False   # using http://
)

# Ensure bucket exists
if not client.bucket_exists(bucket):
    client.make_bucket(bucket)
    print(f"Created bucket: {bucket}")
else:
    print(f"Bucket exists: {bucket}")

# ----------------------------------------------------
# Process CSV files from local raw folder
# ----------------------------------------------------
for root, _, files in os.walk(raw_folder):
    for file in files:
        if file.lower().endswith(".csv"):

            csv_path = os.path.join(root, file)

            # Read CSV
            df = pd.read_csv(csv_path)

            # Table name = file name without extension
            table_name = os.path.splitext(file)[0]

            # MinIO object path (Landing Layer)
            key = f"{landing_prefix}/{table_name}/{today}/data.parquet"

            # Convert DataFrame → Parquet bytes
            table = pa.Table.from_pandas(df)
            buf = pa.BufferOutputStream()
            pq.write_table(table, buf)
            data_bytes = buf.getvalue().to_pybytes()

            # Upload to MinIO
            client.put_object(
                bucket,
                key,
                io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type="application/parquet"
            )

            print(f"Uploaded: {key}  ({len(df)} rows)")
