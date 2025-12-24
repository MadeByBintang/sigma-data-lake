import boto3
import pandas as pd
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# LOAD ENV
load_dotenv()

BUCKET = "sigma-lake"
BRONZE_PREFIX = "bronze/sql/"
SILVER_PREFIX = "silver/sql_cleaned/"

# MINIO CLIENT
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

# LOAD LATEST BRONZE CSV
objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX).get("Contents", [])

if not objects:
    raise RuntimeError("❌ Tidak ada data SQL bronze")

latest_key = sorted(objects, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

obj = s3.get_object(Bucket=BUCKET, Key=latest_key)
df = pd.read_csv(obj["Body"])

print(f"📥 RAW ROWS: {len(df)}")

# CLEANING

# waktu → ambil HH:MM:SS saja
df["waktu"] = df["waktu"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")

# trim text
df["nama_warung"] = df["nama_warung"].str.strip().str.title()
df["menu"] = df["menu"].str.strip()
df["kategori"] = df["kategori"].str.strip().str.title()

# metode → normalisasi
df["metode"] = (
    df["metode"]
    .str.strip()
    .str.lower()
    .replace({"dine in": "dine-in", "dine-in": "dine-in"})
)

# harga & kepuasan
df["harga"] = pd.to_numeric(df["harga"], errors="coerce")
df["kepuasan"] = pd.to_numeric(df["kepuasan"], errors="coerce")

# drop baris rusak
df = df.dropna(subset=["tanggal", "waktu", "harga", "kepuasan"])

df["harga"] = df["harga"].astype(int)
df["kepuasan"] = df["kepuasan"].astype(int)

# SAVE SILVER
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_key = f"{SILVER_PREFIX}sql_cleaned_{timestamp}.json"

output = {
    "source_bronze": latest_key,
    "total_raw": len(df),
    "data": df.to_dict(orient="records"),
}

s3.put_object(
    Bucket=BUCKET,
    Key=output_key,
    Body=json.dumps(output, ensure_ascii=False),
    ContentType="application/json",
)

print("✅ SQL SILVER CLEANED SAVED")
print(f"📊 CLEANED ROWS: {len(df)}")
