FROM python:3.11-slim

WORKDIR /app

COPY fetch_weather.py .
COPY crontab /etc/cron.d/weather-cron
COPY .env .

RUN pip install requests boto3 python-dotenv \
    && apt-get update \
    && apt-get install -y cron \
    && chmod 0644 /etc/cron.d/weather-cron \
    && crontab /etc/cron.d/weather-cron \
    && touch /var/log/cron.log

CMD ["cron", "-f"]
