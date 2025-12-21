import pandas as pd
import boto3
import os
import io
import json
from dotenv import load_dotenv

load_dotenv()

# Config MinIO Internal Docker
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123",
)

BUCKET = "sigma-lake"
SOURCE_KEY = "bronze/master/master_warung.csv"
OUTPUT_KEY = "silver/master/warung_cleaned.json"


def clean_master():
    print("🧹 Cleaning Master Warung (V2 - Atribut Baru)...")

    obj = s3.get_object(Bucket=BUCKET, Key=SOURCE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    # 1. Boolean Conversion
    bool_cols = ["indoor", "pedas"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper() == "TRUE"

    # 2. Numeric Conversion
    num_cols = ["jarak_menit", "harga_rata2", "waktu_saji"]  # Tambah waktu_saji
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # 3. Float Conversion (Rating Rasa)
    df["rating_rasa"] = pd.to_numeric(df["rating_rasa"], errors="coerce").fillna(3.0)

    # 4. String/Category Conversion (Porsi)
    df["porsi"] = df["porsi"].fillna("Sedang").astype(str)

    # Save
    json_data = df.to_dict(orient="records")
    s3.put_object(
        Bucket=BUCKET,
        Key=OUTPUT_KEY,
        Body=json.dumps(json_data),
        ContentType="application/json",
    )
    print(f"✅ Master V2 Saved! ({len(df)} rows)")


if __name__ == "__main__":
    clean_master()
