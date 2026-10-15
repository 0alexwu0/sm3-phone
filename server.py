import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rocksdict import Rdict, AccessType


DB_ROOT = os.getenv(
    "DB_ROOT",
    "/data/rocksdb/phone_sm3"
)

SHARDS = int(
    os.getenv(
        "SHARDS",
        "32"
    )
)

db_list = []


@asynccontextmanager
async def lifespan(app: FastAPI):

    global db_list

    print(f"[INFO] DB_ROOT={DB_ROOT}")
    print(f"[INFO] SHARDS={SHARDS}")

    db_list = []

    for i in range(SHARDS):

        path = os.path.join(
            DB_ROOT,
            f"shard_{i:02d}"
        )

        if not os.path.exists(path):
            raise RuntimeError(
                f"RocksDB shard not found: {path}"
            )

        print(
            f"[INFO] Opening shard_{i:02d}: {path}"
        )

        db = Rdict(
            path,
            access_type=AccessType.read_only()
        )

        db_list.append(db)

    print(
        f"[SUCCESS] Opened {len(db_list)} RocksDB shards"
    )

    yield

    for db in db_list:
        try:
            db.close()
        except Exception:
            pass

    db_list = []


app = FastAPI(
    title="SM3 Phone Lookup API",
    version="2.0.0",
    lifespan=lifespan
)


class QueryRequest(BaseModel):
    sm3: str


def lookup(sm3_hex: str):

    sm3_hex = sm3_hex.strip().lower()

    # SM3 = 32 bytes = 64 hex chars
    if len(sm3_hex) != 64:
        raise HTTPException(
            status_code=400,
            detail="SM3必须是64位十六进制字符串"
        )

    try:
        key = bytes.fromhex(
            sm3_hex
        )
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="SM3格式错误，只允许0-9和a-f"
        )

    # 必须与生成脚本保持一致
    #
    # 生成时:
    # shard = digest[0] & (SHARDS - 1)
    #
    shard = key[0] & (SHARDS - 1)

    db = db_list[shard]

    value = db.get(key)

    if value is None:
        return None, shard

    phone = value.decode(
        "ascii"
    )

    return phone, shard


@app.get("/")
def root():

    return {
        "service": "SM3 Phone Lookup",
        "version": "2.0.0",
        "status": "running",
        "shards": SHARDS
    }


@app.get("/health")
def health():

    return {
        "status": "ok",
        "shards": len(db_list)
    }


@app.get("/query/{sm3}")
def query_get(sm3: str):

    phone, shard = lookup(
        sm3
    )

    if phone is None:
        return {
            "found": False,
            "phone": None
        }

    return {
        "found": True,
        "phone": phone
    }


@app.post("/query")
def query_post(request: QueryRequest):

    phone, shard = lookup(
        request.sm3
    )

    if phone is None:
        return {
            "found": False,
            "phone": None
        }

    return {
        "found": True,
        "phone": phone
    }


@app.get("/debug/{sm3}")
def debug(sm3: str):

    phone, shard = lookup(
        sm3
    )

    return {
        "sm3": sm3,
        "shard": shard,
        "found": phone is not None,
        "phone": phone
    }