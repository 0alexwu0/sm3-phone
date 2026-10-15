FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV TZ=Asia/Shanghai
ENV DB_PATH=/data/rocksdb/phone_sm3

WORKDIR /app

COPY requirements.txt /app/

RUN pip install \
    --no-cache-dir \
    -r requirements.txt

COPY generate.py /app/
COPY server.py /app/
COPY prefixes.txt /app/

RUN mkdir -p /data/rocksdb

EXPOSE 8080

CMD ["uvicorn","server:app","--host","0.0.0.0","--port","8080","--workers","1"]
