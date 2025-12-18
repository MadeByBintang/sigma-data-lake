import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os
import re
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

PROMO_KEYWORDS = [
    "diskon", "voucher", "cashback", "gratis", "ongkir", "promo", "off"
]

# ==============================
# MINIO CLIENT
# ==============================
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

# ==============================
# CLEAN TEXT
# ==============================
def clean_text(text: str) -> str:
    noise_patterns = [
        r"lihat penawaran", r"lihat kode promo", r"lihat detail",
        r"gunakan voucher", r"syarat", r"ketentuan",
        r"arrow[- ]?forwardios", r"arrow[- ]?forward",
        r"diverifikasi", r"offer verified", r"los"
    ]

    cleaned = text
    for pattern in noise_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"[^\w\s%]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned

# ==============================
# SCRAPER
# ==============================
def scrape_food_vouchers(platform, url):
    response = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    results = []
    now = datetime.now()

    elements = soup.find_all(["article", "div", "a"])

    for el in elements:
        raw_text = el.get_text(separator=" ", strip=True)
        if not raw_text:
            continue

        cleaned = clean_text(raw_text)
        text_lower = cleaned.lower()

        if "food" not in text_lower:
            continue

        if not any(k in text_lower for k in PROMO_KEYWORDS):
            continue

        if len(cleaned) > 600:
            continue

        results.append({
            "platform": platform,
            "judul_promo": cleaned,
            "deskripsi": cleaned,
            "tanggal_scrape": now.date().isoformat(),
            "waktu_scrape": now.strftime("%H:%M:%S"),
            "sumber": "Cuponation"
        })

    return results

# ==============================
# MAIN
# ==============================
def main():
    all_vouchers = []

    for route in ROUTES:
        all_vouchers.extend(
            scrape_food_vouchers(route["platform"], route["url"])
        )

    unique = {
        (v["platform"], v["deskripsi"]): v
        for v in all_vouchers
    }.values()

    output = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_voucher": len(unique),
        "data": list(unique)
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    key = f"raw/promo/promo_{timestamp}.json"

    s3.put_object(
        Bucket="sigma-lake",
        Key=key,
        Body=json.dumps(output, ensure_ascii=False),
        ContentType="application/json"
    )

    print(f"Promo data saved to {key}")

if __name__ == "__main__":
    main()
