FROM python:3.11-slim

WORKDIR /app

COPY fetch_promo.py .
COPY crontab /etc/cron.d/promo-cron
COPY .env .

RUN pip install requests beautifulsoup4 boto3 python-dotenv \
    && apt-get update \
    && apt-get install -y cron \
    && chmod 0644 /etc/cron.d/promo-cron \
    && crontab /etc/cron.d/promo-cron \
    && touch /var/log/cron.log

CMD ["cron", "-f"]
