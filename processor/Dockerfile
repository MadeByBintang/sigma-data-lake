FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD sh -c "python silver/weather_cleaned.py && python silver/sql_cleaned.py && python silver/promo_cleaned.py && python gold/decision_binding.py"