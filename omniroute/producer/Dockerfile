FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt

COPY common /app/common
COPY producer /app/producer

WORKDIR /app

CMD ["python", "producer/telemetry_producer.py"]
