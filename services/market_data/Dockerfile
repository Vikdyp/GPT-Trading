# services/market_data/Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Dépendances du service market_data
COPY services/market_data/requirements.txt /app/services/market_data/requirements.txt
RUN pip install --no-cache-dir -r /app/services/market_data/requirements.txt

# Lib partagée + code du service
COPY libs /app/libs
COPY services/market_data /app/services/market_data

# Import path pour "from libs.common import signals"
ENV PYTHONPATH=/app

WORKDIR /app/services/market_data
CMD ["python", "-u", "runner.py"]
