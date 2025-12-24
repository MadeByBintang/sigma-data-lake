import boto3
import pandas as pd
import json
import os
import io
from datetime import datetime, time
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text
from sklearn.metrics import accuracy_score
from dotenv import load_dotenv

# KONFIGURASI ENV & S3
load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

BUCKET = "sigma-lake"


# HELPER: LOAD DATA
def load_dataset_from_prefix(prefix, file_type="json"):
    objects = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    data_list = []
    for obj in objects:
        key = obj["Key"]
        try:
            response = s3.get_object(Bucket=BUCKET, Key=key)
            body = response["Body"].read()
            if file_type == "json":
                content = json.loads(body)
                if isinstance(content, dict) and "data" in content:
                    (
                        data_list.extend(content["data"])
                        if isinstance(content["data"], list)
                        else data_list.extend(content["data"])
                    )
                elif isinstance(content, dict):
                    data_list.append(content)
                elif isinstance(content, list):
                    data_list.extend(content)
            elif file_type == "csv":
                df_temp = pd.read_csv(io.BytesIO(body))
                data_list.append(df_temp)
        except Exception as e:
            print(f"⚠️ Gagal load {key}: {e}")

    if file_type == "csv":
        return pd.concat(data_list, ignore_index=True) if data_list else pd.DataFrame()
    else:
        return pd.DataFrame(data_list)


# 1. LOAD DATA
print("📥 Loading Data Silver...")
df_transaksi = load_dataset_from_prefix("silver/sql_cleaned/", "json")
df_weather = load_dataset_from_prefix("silver/weather_cleaned/", "json")
df_promo = load_dataset_from_prefix("silver/promo_cleaned/", "json")

if df_transaksi.empty:
    raise ValueError("❌ Data Transaksi Kosong!")

# 2. DATA PREPARATION (LOGIKA BARU)
print("🔗 Melakukan Data Binding (Cuaca Saat Ini)...")

# --- A. Siapkan Transaksi ---
df_transaksi["datetime_makan"] = pd.to_datetime(
    df_transaksi["tanggal"] + " " + df_transaksi["waktu"]
)
df_bound = df_transaksi.copy()

# --- B. Siapkan Cuaca (AMBIL TERBARU SAJA) ---
if not df_weather.empty:
    # Urutkan berdasarkan timestamp terbaru -> Ambil baris pertama
    df_weather["timestamp"] = pd.to_datetime(df_weather["timestamp"])
    latest_weather = df_weather.sort_values("timestamp", ascending=False).iloc[0]

    print(
        f"⛅ Menggunakan Cuaca Terbaru: {latest_weather['timestamp']} | {latest_weather['kondisi']} | {latest_weather['suhu']}°C"
    )

    # Tempelkan ke SEMUA baris transaksi (Broadcasting)
    df_bound["kondisi"] = latest_weather["kondisi"]
    df_bound["suhu"] = latest_weather["suhu"]
    df_bound["kelembapan"] = latest_weather["kelembapan"]
else:
    print("⚠️ Tidak ada data cuaca. Menggunakan nilai default.")
    df_bound["kondisi"] = "Unknown"
    df_bound["suhu"] = 30.0
    df_bound["kelembapan"] = 70.0

# --- C. Siapkan Promo (Tetap Historical / Harian) ---
# Promo tetap dicek per hari transaksi, karena aneh kalau promo hari ini dipaksa ke transaksi lalu.
if not df_promo.empty:
    df_promo["tanggal_scrape"] = pd.to_datetime(df_promo["tanggal_scrape"]).dt.date
    promo_per_day = (
        df_promo.groupby("tanggal_scrape").size().reset_index(name="jumlah_promo")
    )

    df_bound["tanggal_date"] = df_bound["datetime_makan"].dt.date
    df_bound = pd.merge(
        df_bound,
        promo_per_day,
        left_on="tanggal_date",
        right_on="tanggal_scrape",
        how="left",
    )
    df_bound["jumlah_promo"] = df_bound["jumlah_promo"].fillna(0)
else:
    df_bound["jumlah_promo"] = 0

# 3. FEATURE ENGINEERING
print("🛠️ Membuat Fitur Keputusan...")

HUJAN_KEYWORDS = ["rain", "drizzle", "thunderstorm", "storm"]
df_bound["is_hujan"] = (
    df_bound["kondisi"]
    .astype(str)
    .apply(lambda x: 1 if any(k in x.lower() for k in HUJAN_KEYWORDS) else 0)
)

df_bound["ada_promo"] = (df_bound["jumlah_promo"] > 0).astype(int)


def check_lunch(dt):
    t = dt.time()
    return 1 if time(11, 0) <= t <= time(14, 0) else 0


df_bound["is_lunch_time"] = df_bound["datetime_makan"].apply(check_lunch)

features = ["harga", "is_hujan", "suhu", "ada_promo", "is_lunch_time"]
target = "kepuasan"
df_final = df_bound.dropna(subset=[target] + ["harga"])

# 4. TRAINING MODEL
print(f"🤖 Melatih Model Decision Tree dengan {len(df_final)} data...")

if len(df_final) < 5:
    X_train = df_final[features]
    y_train = df_final[target]
    X_test, y_test = None, None
else:
    X_train, X_test, y_train, y_test = train_test_split(
        df_final[features], df_final[target], test_size=0.2, random_state=42
    )

model = DecisionTreeClassifier(max_depth=4, criterion="entropy", random_state=42)
model.fit(X_train, y_train)

# 5. EVALUASI & RULES
if X_test is not None:
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    print(f"🎯 Akurasi Model: {acc:.2f}")
else:
    print("🎯 Akurasi Model: N/A (Full Training)")

print("\n📜 Decision Rules (Logika Keputusan):")
print(export_text(model, feature_names=features))

# Simpan hasil
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_key = f"gold/decision_binding/data_bound_{timestamp}.csv"
csv_buffer = io.StringIO()
df_final.to_csv(csv_buffer, index=False)
s3.put_object(
    Bucket=BUCKET, Key=output_key, Body=csv_buffer.getvalue(), ContentType="text/csv"
)
print(f"✅ Data Gold tersimpan di {output_key}")
