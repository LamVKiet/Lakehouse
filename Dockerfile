# Lightweight Python image for ingestion services:
#   - data-simulator (FastAPI)
#   - producer (Kafka client)
#   - consumers (Kafka client)
# No Java/Spark needed — keeps image small (~150MB).
FROM python:3.10-slim

WORKDIR /app

# Create and activate a virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Project root on PYTHONPATH so "from common.kafka_config import ..." works
ENV PYTHONPATH=/app

CMD ["python", "--version"]
