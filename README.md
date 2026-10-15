# sm3-phone

> 手机号 SM3 哈希的逆向查找（rainbow table）工具 —— 用于演示「对低熵个人信息（如手机号）做哈希并不能真正保护隐私」。

`sm3-phone` 会穷举指定号段下的全部中国大陆手机号，计算其 [SM3](https://en.wikipedia.org/wiki/SM3_(hash_function)) 哈希，并以哈希为 key、手机号为 value 写入分片的 [RocksDB](https://rocksdb.org/)，再通过一个轻量 HTTP API 提供「SM3 哈希 → 原始手机号」的反查。

手机号的取值空间很小（11 位、号段受限，总量约 10 位数量级），因此对其做任何**无盐、无密钥**的哈希（SM3 / MD5 / SHA-256 等）都可以被完整穷举并建成反查表。本项目正是为了直观地说明这一点。

## ⚠️ 合规与免责声明

本项目仅供**授权范围内的安全研究、密码学教学与隐私风险演示**使用。

- 它证明的是一个**防御性**结论：对手机号这类低熵个人信息，仅做哈希脱敏是不安全的，应改用带密钥的 HMAC、加盐 + 慢哈希、加密存储或集中化的脱敏/令牌化服务。
- **禁止**将本项目用于反查、去匿名化、追踪或以任何方式处理你未获合法授权的个人信息。
- 在中国大陆，手机号属于《个人信息保护法》《数据安全法》等法律保护的个人信息；未经授权的收集、反查、交易或滥用可能构成违法乃至犯罪。各地法律不同，使用前请自行确认合规性。
- 作者与贡献者对任何人以任何方式使用本项目所造成的后果**不承担任何责任**。下载或使用即表示你已阅读并同意上述条款，并将自行承担全部法律与合规责任。

本仓库**不包含**任何已生成的数据库或手机号数据，只包含生成与查询的代码。

## 工作原理

分为两个阶段：

1. **生成（`generate.py`）**
   - 从 `prefixes.txt` 读取号段前缀，穷举每个前缀下的所有手机号。
   - 多进程并行计算 SM3，按 `digest[0] & (SHARDS - 1)` 分片先写入临时 spool 文件。
   - 第二阶段把各 shard 的 spool 文件批量灌入对应的 RocksDB（key = 32 字节 SM3，value = 11 字节手机号）。
2. **查询（`server.py`）**
   - 以只读方式打开全部 shard，提供 FastAPI 接口。
   - 查询时按同样的分片规则定位 shard，直接以哈希为 key 查 RocksDB。

> SM3 需要底层 OpenSSL 支持（`hashlib.new("sm3", ...)`）。较新的 OpenSSL 3.x 默认支持。

## 环境要求

- Python 3.11+
- 支持 SM3 的 OpenSSL（可用 `python -c 'import hashlib; print(hashlib.new("sm3", b"x").hexdigest())'` 验证）
- 足够的磁盘空间（全量号段的数据库可达数十 GiB，取决于号段数量）

## 安装

```bash
pip install -r requirements.txt
```

## 生成数据库

```bash
python generate.py \
  --prefix-file prefixes.txt \
  --db-root /path/to/rocksdb/phone_sm3 \
  --tmp-root /path/to/rocksdb_build_tmp \
  --workers 30 \
  --builder-workers 8 \
  --shards 32
```

常用参数：

| 参数 | 说明 | 默认 |
| --- | --- | --- |
| `--prefix-file` | 号段前缀文件（必填） | — |
| `--db-root` | RocksDB 输出目录 | `/data/rocksdb/phone_sm3` |
| `--tmp-root` | 临时 spool 目录 | `/data/rocksdb_build_tmp` |
| `--workers` | SM3 计算进程数 | 30 |
| `--builder-workers` | RocksDB 灌库进程数 | 8 |
| `--shards` | 分片数（**必须是 2 的幂**） | 32 |
| `--chunk-size` | 每个计算任务的手机号数量 | 500000 |
| `--batch-size` | RocksDB WriteBatch 大小 | 200000 |
| `--keep-temp` | 保留临时文件 | 关 |
| `--phase2-only` | 跳过计算，仅灌库 | 关 |

`prefixes.txt` 每行一个号段前缀（如 `139`），支持 `#` 注释行。

## 启动查询服务

```bash
DB_ROOT=/path/to/rocksdb/phone_sm3 SHARDS=32 \
  uvicorn server:app --host 0.0.0.0 --port 8080
```

环境变量 `DB_ROOT` 与 `SHARDS` 必须与生成时一致。

### API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 服务信息 |
| `GET` | `/health` | 健康检查 |
| `GET` | `/query/{sm3}` | 按 SM3 哈希反查 |
| `POST` | `/query` | 请求体 `{"sm3": "<64位hex>"}` |
| `GET` | `/debug/{sm3}` | 返回命中的 shard 等调试信息 |

示例：

```bash
# 13800138000 的 SM3
SM3=$(python -c 'import hashlib;print(hashlib.new("sm3",b"13800138000").hexdigest())')
curl "http://localhost:8080/query/$SM3"
# => {"found": true, "phone": "13800138000"}   (若该号在库中)
```

## Docker

```bash
docker build -t sm3-phone .
docker run --rm -p 8080:8080 \
  -v /path/to/rocksdb/phone_sm3:/data/rocksdb/phone_sm3 \
  sm3-phone
```

镜像内已包含 `generate.py` 与 `server.py`；默认启动查询服务，数据库需通过 volume 挂载（或先在容器内生成）。

## 许可证

本项目以 [Apache License 2.0](LICENSE) 开源。详见 [LICENSE](LICENSE) 与 [NOTICE](NOTICE)。
