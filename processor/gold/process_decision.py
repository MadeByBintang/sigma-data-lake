import boto3
import pandas as pd
import json
import os
from io import StringIO
from dotenv import load_dotenv

# ==============================
# LOAD ENV
# ==============================
load_dotenv()

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
# LOAD WEATHER (LATEST - BRONZE)
# ==============================
weather_objs = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/weather/").get(
    "Contents", []
)

latest_weather_key = sorted(
    weather_objs, key=lambda x: x["LastModified"], reverse=True
)[0]["Key"]

weather_json = json.loads(
    s3.get_object(Bucket=BUCKET, Key=latest_weather_key)["Body"].read()
)

# ==============================
# LOAD SQL CSV (LATEST - BRONZE)
# ==============================
sql_objs = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/sql/").get("Contents", [])

latest_sql_key = sorted(sql_objs, key=lambda x: x["LastModified"], reverse=True)[0][
    "Key"
]

sql_csv = (
    s3.get_object(Bucket=BUCKET, Key=latest_sql_key)["Body"].read().decode("utf-8")
)

df_sql = pd.read_csv(StringIO(sql_csv))

# ==============================
# LOAD PROMO (LATEST - BRONZE)
# ==============================
promo_objs = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/promo/").get(
    "Contents", []
)

latest_promo_key = sorted(promo_objs, key=lambda x: x["LastModified"], reverse=True)[0][
    "Key"
]

promo_json = json.loads(
    s3.get_object(Bucket=BUCKET, Key=latest_promo_key)["Body"].read()
)

df_promo = pd.DataFrame(promo_json["data"])

# ==============================
# FEATURE ENGINEERING (SILVER)
# ==============================
df_sql["harga"] = df_sql["harga"].astype(int)
df_sql["kepuasan"] = df_sql["kepuasan"].astype(int)

# --- WEATHER EXTRACTION ---
kondisi = weather_json["weather"][0]["main"].lower()
suhu = float(weather_json["main"]["temp"])
kelembapan = int(weather_json["main"]["humidity"])

df_sql["is_hujan"] = int(any(x in kondisi for x in ["rain", "drizzle", "thunderstorm"]))

df_sql["suhu"] = suhu
df_sql["kelembapan"] = kelembapan

# --- PROMO FLAG ---
df_sql["ada_promo"] = 1 if len(df_promo) > 0 else 0

# ==============================
# SAVE TO SILVER
# ==============================
output_key = "silver/decision/food_decision.csv"

csv_buffer = StringIO()
df_sql.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=BUCKET, Key=output_key, Body=csv_buffer.getvalue(), ContentType="text/csv"
)

print(f"✅ SILVER data saved to {output_key}")
