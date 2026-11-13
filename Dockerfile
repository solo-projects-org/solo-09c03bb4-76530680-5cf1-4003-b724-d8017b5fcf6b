FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV DATA_DIR=/data \
    MAX_FILE_BYTES=67108864

VOLUME ["/data"]
EXPOSE 8000

# Single process / single worker: all in-memory coordination assumes one process.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
