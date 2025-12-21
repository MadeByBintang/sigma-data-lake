import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os
import boto3
from dotenv import load_dotenv

# ==============================
# LOAD ENV
# ==============================
load_dotenv()

# ==============================
# KONFIGURASI
# ==============================
ROUTES = [
    {"platform": "GoJek", "url": "https://www.cuponation.co.id/gojek-voucher"},
    {"platform": "Grab", "url": "https://www.cuponation.co.id/grabfood"},
    {"platform": "Shopee", "url": "https://www.cuponation.co.id/shopee-kode-promo"},
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# ==============================
# MINIO CLIENT
# ==============================
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

BUCKET = "sigma-lake"


# ==============================
# SCRAPE RAW
# ==============================
def scrape_raw(platform, url):
    response = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    raw_items = []
    now = datetime.now()

    for el in soup.find_all(["article", "div", "a"]):
        text = el.get_text(separator=" ", strip=True)
        if not text:
            continue

        raw_items.append(
            {
                "platform": platform,
                "raw_text": text,
                "scrape_date": now.date().isoformat(),
                "scrape_time": now.strftime("%H:%M:%S"),
                "source_url": url,
            }
        )

    return raw_items


# ==============================
# MAIN
# ==============================
def main():
    all_raw = []

    for route in ROUTES:
        all_raw.extend(scrape_raw(route["platform"], route["url"]))

    output = {
        "ingest_time": datetime.now().isoformat(),
        "source": "Cuponation",
        "data": all_raw,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    key = f"bronze/promo/promo_raw_{timestamp}.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(output, ensure_ascii=False),
        ContentType="application/json",
    )

    print(f"✅ RAW promo data saved to {key}")


if __name__ == "__main__":
    main()
