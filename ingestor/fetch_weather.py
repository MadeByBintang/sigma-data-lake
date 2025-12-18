import requests
import json
from datetime import datetime
import boto3
import os
from dotenv import load_dotenv

# load env for cron
load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY", "Banjarmasin")

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
)

# fetch weather
url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&units=metric&appid={API_KEY}"
response = requests.get(url, timeout=10)
data = response.json()

# filename
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"raw/weather/weather_{timestamp}.json"

# upload
s3.put_object(
    Bucket="sigma-lake",
    Key=filename,
    Body=json.dumps(data),
    ContentType="application/json",
)

print(f"Weather data saved to {filename}")
