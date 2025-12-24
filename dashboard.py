import streamlit as st
import pandas as pd
import boto3
import json
import os
import io
from datetime import datetime
from sklearn.tree import DecisionTreeClassifier
from dotenv import load_dotenv

load_dotenv()
st.set_page_config(page_title="SPK Cerdas: Makan Siang", layout="wide")


# 1. KONEKSI & LOAD DATA
@st.cache_resource
def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    if "minio" in endpoint:
        endpoint = "http://localhost:9000"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
    )


@st.cache_data
def load_data():
    s3 = get_s3_client()
    bucket = "sigma-lake"

    # Gold Data
    objs = s3.list_objects_v2(Bucket=bucket, Prefix="gold/decision_binding/").get(
        "Contents", []
    )
    if not objs:
        return None, None, None
    latest_gold = sorted(objs, key=lambda x: x["LastModified"], reverse=True)[0]["Key"]
    obj_gold = s3.get_object(Bucket=bucket, Key=latest_gold)
    df_train = pd.read_csv(io.BytesIO(obj_gold["Body"].read()))

    # Master Warung
    obj_master = s3.get_object(Bucket=bucket, Key="silver/master/warung_cleaned.json")
    master_warung = pd.DataFrame(json.loads(obj_master["Body"].read()))

    # Promo
    objs_promo = s3.list_objects_v2(Bucket=bucket, Prefix="silver/promo_cleaned/").get(
        "Contents", []
    )
    df_promo = pd.DataFrame()
    if objs_promo:
        latest_promo = sorted(
            objs_promo, key=lambda x: x["LastModified"], reverse=True
        )[0]["Key"]
        obj_p = s3.get_object(Bucket=bucket, Key=latest_promo)
        promo_data = json.loads(obj_p["Body"].read())
        df_promo = pd.DataFrame(promo_data["data"])

    return df_train, master_warung, df_promo


try:
    df_train, df_warung, df_promo = load_data()
    if df_train is None:
        st.stop()
except:
    st.stop()

# 2. TRAINING MODEL
if "metode" in df_train.columns:
    df_train["is_takeaway"] = df_train["metode"].apply(
        lambda x: 1 if str(x).lower() == "takeaway" else 0
    )
else:
    df_train["is_takeaway"] = 0

features = ["harga", "is_hujan", "suhu", "ada_promo", "is_lunch_time", "is_takeaway"]
target = "kepuasan"
df_clean = df_train.dropna(subset=features + [target])
model = DecisionTreeClassifier(max_depth=5, random_state=42)
model.fit(df_clean[features], df_clean[target])

# 3. SIDEBAR & PREFERENSI USER
st.title("🍽️ SPK Next-Gen: Makan Apa?")

# Context
latest_context = df_train.iloc[-1]
is_hujan_now = latest_context["is_hujan"]
suhu_now = latest_context["suhu"]
jam_sekarang = st.sidebar.time_input("Jam Sekarang", datetime.now().time())
is_lunch_now = 1 if 11 <= jam_sekarang.hour <= 14 else 0

st.sidebar.metric(
    "Cuaca", "Hujan 🌧️" if is_hujan_now else "Cerah ☀️", f"{suhu_now:.1f}°C"
)

st.sidebar.header("🎯 Mode Prioritas")
mode_pilihan = st.sidebar.radio(
    "Lagi pengen apa?",
    [
        "Seimbang (AI)",
        "Sultan (Sepuasnya)",
        "Tanggal Tua (Hemat)",
        "Kepepet (Cepat)",
    ],
)

st.sidebar.header("Filter & Preferensi")

# --- LOGIKA UI SIDEBAR DINAMIS ---
max_budget = 999999999  # Default Infinity
filter_porsi = []
max_jarak = 10

if mode_pilihan == "Sultan (Sepuasnya)":
    st.info("😎 **Mode Sultan:** Harga bukan masalah.")
    filter_porsi = st.sidebar.multiselect(
        "Mau Porsi Seberapa?", ["Sedang", "Besar", "Jumbo"], default=["Besar", "Jumbo"]
    )
    max_jarak = st.sidebar.slider("Max Jarak (menit)", 1, 20, 15)

elif mode_pilihan == "Tanggal Tua (Hemat)":
    st.info("💸 **Mode Hemat:** Cari yang murah meriah.")
    # Budget slider dihilangkan, sistem otomatis cari yang murah
    max_jarak = st.sidebar.slider("Max Jarak (menit)", 1, 15, 10)

elif mode_pilihan == "Kepepet (Cepat)":
    st.info("⚡ **Mode Cepat:** Filter waktu < 15 menit.")
    max_budget = st.sidebar.slider("Max Budget", 10000, 50000, 25000, 1000)
    max_jarak = 5  # Jarak otomatis dikunci dekat
    st.sidebar.caption("🔒 Jarak dikunci max 5 menit agar cepat.")

else:  # Mode Seimbang
    max_budget = st.sidebar.slider("Max Budget", 10000, 50000, 25000, 1000)
    max_jarak = st.sidebar.slider("Max Jarak (menit)", 1, 20, 10)


# 4. DECISION LOGIC (SCENARIO BASED)
results = []
active_platforms = df_promo["platform"].unique().tolist() if not df_promo.empty else []
promo_avail = 1 if active_platforms else 0

for idx, row in df_warung.iterrows():
    # --- 4.1 Filter Mutlak (Hard Constraints) ---
    # Cuaca
    if is_hujan_now and not row["indoor"]:
        continue

    # Jarak
    if row["jarak_menit"] > max_jarak:
        continue

    # Filter Budget (Hanya untuk Mode Seimbang & Kepepet)
    if (
        mode_pilihan in ["Seimbang (AI)", "Kepepet (Cepat)"]
        and row["harga_rata2"] > max_budget
    ):
        continue

    # Filter Porsi (Khusus Sultan)
    if mode_pilihan == "Sultan (Sepuasnya)" and filter_porsi:
        if row["porsi"] not in filter_porsi:
            continue

    # --- 4.2 PREDIKSI AI (Base Score) ---
    input_base = pd.DataFrame(
        [[row["harga_rata2"], is_hujan_now, suhu_now, promo_avail, is_lunch_now, 0]],
        columns=features,
    )

    prob_kepuasan = (
        model.predict_proba(input_base)[0][1] if len(model.classes_) > 1 else 1.0
    )

    # --- 4.3 SKORING BERBASIS SKENARIO (Weighted Scoring) ---
    final_score = prob_kepuasan * 100  # Skala 0-100
    tags = []  # Untuk label di UI

    # A. LOGIKA SULTAN (Rasa + Porsi)
    if mode_pilihan == "Sultan (Sepuasnya)":
        # Fokus Rasa & Kualitas
        score_rasa = (row["rating_rasa"] / 5.0) * 60  # Bobot rasa dominan (60%)
        final_score = (prob_kepuasan * 40) + score_rasa

        # Bonus Sultan
        if row["rating_rasa"] >= 4.7:
            final_score += 15
            tags.append("TOP TIER ⭐")

    # B. LOGIKA TANGGAL TUA (Harga + Porsi)
    elif mode_pilihan == "Tanggal Tua (Hemat)":
        # Algoritma Pencari Murah: Semakin murah, skor makin tinggi
        # Misal harga 10rb dapet poin penuh, harga 50rb poin 0
        price_sensitivity = max(0, 50000 - row["harga_rata2"]) / 500
        final_score = (prob_kepuasan * 30) + price_sensitivity  # AI cuma 30%, Harga 70%

        if row["harga_rata2"] <= 15000:
            tags.append("HEMAT")

        if row["porsi"] in ["Besar", "Jumbo"] and row["harga_rata2"] <= 18000:
            final_score += 20
            tags.append("KENYANG MAX")

    # C. LOGIKA KEPEPET (Waktu + Jarak)
    elif mode_pilihan == "Kepepet (Cepat)":
        # Prioritas Waktu Saji
        if row["waktu_saji"] <= 5:
            final_score += 40
            tags.append("KILAT ⚡")
        elif row["waktu_saji"] <= 10:
            final_score += 20

        # Penalty Waktu Lama
        if row["waktu_saji"] > 15:
            final_score -= 50

    results.append(
        {
            "data": row,
            "Nama": row["nama_warung"],
            "Harga": row["harga_rata2"],
            "Skor": final_score,
            "Rasa": row["rating_rasa"],
            "Porsi": row["porsi"],
            "Waktu": row["waktu_saji"],
            "Tags": tags,
        }
    )

# 5. TAMPILAN PRESCRIPTIVE
if results:
    df_res = pd.DataFrame(results).sort_values("Skor", ascending=False)
    best = df_res.iloc[0]
    best_data = best["data"]

    # UI Header
    st.success(f"🏆 REKOMENDASI: {mode_pilihan.upper()}")

    c1, c2, c3 = st.columns([1, 1, 2])

    with c1:
        st.subheader(best["Nama"])
        st.caption(f"⭐ {best['Rasa']} | 🍛 Porsi {best['Porsi']}")
        st.markdown(f"**Rp {best['Harga']:,}**")
        if best["Tags"]:
            st.write(" ".join([f"`{t}`" for t in best["Tags"]]))

    with c2:
        st.metric("Estimasi Waktu", f"{best['Waktu']} menit", "Penyajian")
        st.metric("Jarak", f"{best_data['jarak_menit']} menit", "Perjalanan")

    with c3:
        st.info("💡 **ANALISIS PRESCRIPTIVE**")

        # 1. Analisis Skenario Spesifik
        if mode_pilihan == "Sultan (Sepuasnya)":
            st.write("👑 **Status Sultan:**")
            st.write(f"Rating rasa **{best['Rasa']}**/5.0.")
            if best["Porsi"] == "Jumbo":
                st.write("✅ Porsi JUMBO. Cocok buat yang lagi lapar berat.")
            else:
                st.write("✅ Kualitas rasa terjamin.")

        elif mode_pilihan == "Kepepet (Cepat)":
            total_waktu = best["Waktu"] + best_data["jarak_menit"]
            st.write(f"⏱️ **Total Waktu: {total_waktu} menit**")
            if total_waktu <= 15:
                st.write("✅ Aman! Masih keburu sebelum aktivitas selanjutnya.")
            else:
                st.warning("⚠️ Agak mepet. Pertimbangkan takeaway.")

        elif mode_pilihan == "Tanggal Tua (Hemat)":
            st.write("💸 **Analisis Value:**")
            st.write(f"Harga Rp {best['Harga']:,} ramah di kantong.")
            if best["Porsi"] in ["Besar", "Jumbo"]:
                st.write("✅ Value for Money Tinggi! Murah + Banyak.")

        # 2. Saran Metode & Promo (Cross-Check)
        input_tk = pd.DataFrame(
            [[best["Harga"], is_hujan_now, suhu_now, promo_avail, is_lunch_now, 1]],
            columns=features,
        )
        input_di = pd.DataFrame(
            [[best["Harga"], is_hujan_now, suhu_now, promo_avail, is_lunch_now, 0]],
            columns=features,
        )

        prob_tk = model.predict_proba(input_tk)[0][1]
        prob_di = model.predict_proba(input_di)[0][1]

        if prob_tk > prob_di or mode_pilihan == "Kepepet (Cepat)":
            st.write("🥡 **Saran:** Lebih baik **Takeaway**.")
            if active_platforms:
                st.write(f"   🏷️ Cek promo di: **{', '.join(active_platforms)}**")
        else:
            st.write("🍽️ **Saran:** Lebih nikmat **Dine-in**.")

    # List Lainnya
    st.divider()
    st.caption("Opsi Alternatif:")
    st.dataframe(
        df_res.iloc[1:].head(5)[["Nama", "Harga", "Rasa", "Porsi", "Waktu", "Skor"]],
        hide_index=True,
    )

else:
    st.error(
        "Tidak ada warung yang sesuai kriteria. Coba ubah filter Porsi atau Jarak."
    )
