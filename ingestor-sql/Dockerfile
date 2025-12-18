FROM python:3.11-slim

WORKDIR /app

# Install system dependency
RUN apt-get update \
    && apt-get install -y cron \
    && rm -rf /var/lib/apt/lists/*

# Install python dependency (WAJIB ada sqlalchemy)
RUN pip install --no-cache-dir \
    pandas \
    mysql-connector-python \
    sqlalchemy \
    boto3 \
    python-dotenv

# Copy app files
COPY export_sql.py .
COPY crontab /etc/cron.d/sql-cron
COPY .env .

# Setup cron
RUN chmod 0644 /etc/cron.d/sql-cron \
    && crontab /etc/cron.d/sql-cron \
    && touch /var/log/cron.log

# Run cron + keep container alive
CMD cron && tail -f /var/log/cron.log
