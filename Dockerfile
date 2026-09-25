FROM python:3.13-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        default-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV TIKTOKEN_CACHE_DIR=/opt/tiktoken-cache
RUN python -c "import tiktoken; tiktoken.get_encoding('o200k_base')"

COPY app ./app
COPY alembic.ini .
COPY migrations ./migrations

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
