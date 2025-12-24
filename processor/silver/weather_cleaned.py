import boto3
import json
import os
from datetime import datetime
from dotenv import load_dotenv

# LOAD ENV
load_dotenv()

# MINIO CLIENT
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

BUCKET = "sigma-lake"

# LOAD LATEST BRONZE WEATHER
objects = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/weather/").get(
    "Contents", []
)

if not objects:
    raise RuntimeError("❌ Tidak ada data weather di bronze")

latest_key = sorted(objects, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]

raw_weather = json.loads(s3.get_object(Bucket=BUCKET, Key=latest_key)["Body"].read())

# CLEAN & TRANSFORM (SILVER)
cleaned_weather = {
    "kota": raw_weather.get("name"),
    "kondisi": raw_weather["weather"][0]["main"],
    "deskripsi": raw_weather["weather"][0]["description"],
    "suhu": float(raw_weather["main"]["temp"]),
    "kelembapan": int(raw_weather["main"]["humidity"]),
    "timestamp": datetime.now().isoformat(),
}

# SAVE TO SILVER
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_key = f"silver/weather_cleaned/weather_cleaned_{timestamp}.json"

s3.put_object(
    Bucket=BUCKET,
    Key=output_key,
    Body=json.dumps(cleaned_weather, ensure_ascii=False),
    ContentType="application/json",
)

print(f"✅ Weather cleaned saved to {output_key}")
