FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kvstore/ kvstore/

ENV KVSTORE_WAL_PATH=/data/kvstore.wal
VOLUME ["/data"]

EXPOSE 8000

CMD ["uvicorn", "kvstore.server:app", "--host", "0.0.0.0", "--port", "8000"]
