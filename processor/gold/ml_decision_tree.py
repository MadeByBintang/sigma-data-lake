import pandas as pd
import boto3, os
from io import StringIO
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score, classification_report
from dotenv import load_dotenv

load_dotenv()

# ======================
# MINIO CLIENT
# ======================
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

BUCKET = "sigma-lake"
KEY = "silver/decision/food_decision.csv"

# ======================
# LOAD DATA
# ======================
csv = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read().decode()
df = pd.read_csv(StringIO(csv))

# ======================
# FEATURES & TARGET
# ======================
X = df[["harga", "is_hujan", "suhu", "kelembapan", "ada_promo"]]
y = df["kepuasan"]

# ======================
# SPLIT
# ======================
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ======================
# TRAIN MODEL
# ======================
model = DecisionTreeClassifier(max_depth=4, random_state=42)
model.fit(X_train, y_train)

# ======================
# EVALUATION
# ======================
y_pred = model.predict(X_test)

print("🎯 Accuracy:", accuracy_score(y_test, y_pred))
print("\n📊 Classification Report:\n", classification_report(y_test, y_pred))
