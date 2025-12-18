import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import boto3
import os
from dotenv import load_dotenv
from io import StringIO

# ==============================
# LOAD ENV
# ==============================
load_dotenv()

# ==============================
# MYSQL CONFIG
# ==============================
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# ==============================
# SQLALCHEMY ENGINE
# ==============================
engine = create_engine(
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)

# ==============================
# MINIO CONFIG
# ==============================
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

# ==============================
# EXPORT QUERY
# ==============================
query = """
SELECT
    tanggal,
    waktu,
    nama_warung,
    menu,
    kategori,
    harga,
    metode,
    kepuasan
FROM riwayat_makan
"""

df = pd.read_sql(query, engine)

# ==============================
# SAVE TO MINIO
# ==============================
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
key = f"raw/sql/riwayat_makan_{timestamp}.csv"

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket="sigma-lake", Key=key, Body=csv_buffer.getvalue(), ContentType="text/csv"
)

print(f"SQL data saved to {key}")
