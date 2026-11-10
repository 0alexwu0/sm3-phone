FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Asia/Shanghai \
    DB_ROOT=/data/rocksdb/phone_sm3 \
    SHARDS=32

WORKDIR /app

COPY requirements.txt /app/

RUN pip install -r requirements.txt \
    # 构建期确认 OpenSSL 提供 SM3，避免运行时才发现生成/查询无法工作
    && python -c "import hashlib; hashlib.new('sm3', b'')"

COPY generate.py append.py server.py prefixes.txt /app/

# 数据库目录声明为数据卷：生成/追加的数据必须落在挂载的宿主机目录上，
# 否则会随容器删除而丢失。
RUN mkdir -p /data/rocksdb
VOLUME ["/data/rocksdb"]

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3)" || exit 1

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
