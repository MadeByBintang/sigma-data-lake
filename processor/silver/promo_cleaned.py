import boto3
import json
import os
import re
from datetime import datetime
from dotenv import load_dotenv

# LOAD ENV
load_dotenv()

BUCKET = "sigma-lake"
BRONZE_PREFIX = "bronze/promo/"
SILVER_PREFIX = "silver/promo_cleaned/"

VALID_PLATFORMS = {"GoJek", "Grab", "Shopee"}

PROMO_KEYWORDS = ["diskon", "voucher", "cashback", "gratis", "ongkir", "promo", "off"]

# MINIO CLIENT
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)


# CLEAN TEXT (TANPA LOWERCASE)
def clean_text(text: str) -> str:
    noise_patterns = [
        r"lihat penawaran",
        r"lihat kode promo",
        r"lihat detail",
        r"gunakan voucher",
        r"syarat",
        r"ketentuan",
        r"arrow[- ]?forwardios",
        r"arrow[- ]?forward",
        r"diverifikasi",
        r"offer verified",
        r"los",
    ]

    cleaned = text  # ❗ TIDAK lower
    for pattern in noise_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"[^\w\s%]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # Optional: rapikan RP
    cleaned = re.sub(r"\brp\b", "RP", cleaned, flags=re.IGNORECASE)

    return cleaned


# LOAD LATEST BRONZE
objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=BRONZE_PREFIX).get("Contents", [])

if not objects:
    raise RuntimeError("❌ Tidak ada data promo bronze")

latest_key = sorted(objects, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

raw = json.loads(s3.get_object(Bucket=BUCKET, Key=latest_key)["Body"].read())

rows = raw.get("data", [])

print(f"📥 RAW ROWS: {len(rows)}")

# CLEAN + FILTER (SILVER)
cleaned_rows = []

for row in rows:
    platform = row.get("platform")
    raw_text = row.get("raw_text", "")

    if platform not in VALID_PLATFORMS:
        continue

    cleaned = clean_text(raw_text)
    text_lower = cleaned.lower()  # ✅ hanya untuk filter

    if "food" not in text_lower:
        continue

    if not any(k in text_lower for k in PROMO_KEYWORDS):
        continue

    if len(cleaned) < 20 or len(cleaned) > 600:
        continue

    cleaned_rows.append(
        {
            "platform": platform,
            "judul_promo": cleaned,
            "kata_kunci": [k for k in PROMO_KEYWORDS if k in text_lower],
            "tanggal_scrape": row.get("scrape_date"),
            "waktu_scrape": row.get("scrape_time"),
            "source_url": row.get("source_url"),
            "timestamp": datetime.now().isoformat(),
        }
    )

# SAVE SILVER
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_key = f"{SILVER_PREFIX}promo_cleaned_{timestamp}.json"

output = {
    "source_bronze": latest_key,
    "total_raw": len(rows),
    "total_cleaned": len(cleaned_rows),
    "data": cleaned_rows,
}

s3.put_object(
    Bucket=BUCKET,
    Key=output_key,
    Body=json.dumps(output, ensure_ascii=False),
    ContentType="application/json",
)

print("✅ SILVER promo cleaned saved")
print(f"📊 CLEANED: {len(cleaned_rows)}")
