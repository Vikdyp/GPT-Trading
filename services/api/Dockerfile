FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/api/requirements.txt /app/services/api/requirements.txt
RUN pip install --no-cache-dir -r /app/services/api/requirements.txt

COPY libs /app/libs
COPY services/api /app/services/api

ENV CONFIG_PATH=/app/config/app.yaml
ENV PYTHONPATH=/app

WORKDIR /app/services/api
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
