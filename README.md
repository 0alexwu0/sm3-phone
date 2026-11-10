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

1. **生成（`generate.py`）** —— 单遍流式流水线
   - 从 `prefixes.txt` 读取号段前缀（UTF-8 / BOM 兼容，自动去重，仅接受 1~10 位 ASCII 数字），穷举每个前缀下的所有手机号。
   - 多个 worker 进程（`spawn` 启动）并行计算 SM3，每个任务只返回一块**有界**的内存数据，**不再写临时 spool 文件**。
   - 默认由**单个写入进程**（`ShardWriter`）拥有全部 shard，按 `digest[0] & (SHARDS - 1)` 分片，边生成边写入对应 RocksDB（key = 32 字节 SM3，value = 11 字节手机号）。所有 shard 共享同一个 block cache 与 write buffer manager，从而让整体内存受统一预算约束。
   - 可用 `--builder-workers N`（N>1）开启**多个并行写入进程**：shard 按 `shard % N` 分配给各进程，路由在 worker 内按 `digest[0]` 直接分桶完成，主进程只做分发。磁盘/IO 成为瓶颈时可提升写入吞吐——代价是跨进程无法共享 Cache，缓存预算会按 builder 数平分（每进程 `预算/N`）。
   - 保留 WAL 且 `sync=False`：已提交批次可在进程异常退出后恢复，正常 compaction / stall 阈值提供磁盘背压。
2. **查询（`server.py`）**
   - 以只读方式打开全部 shard，提供 FastAPI 接口。
   - 查询时按同样的分片规则定位 shard，直接以哈希为 key 查 RocksDB。

> SM3 需要底层 OpenSSL 支持（`hashlib.new("sm3", ...)`）。较新的 OpenSSL 3.x 默认支持。

### 内存预算（自适应）

生成阶段会**按机器可用内存自动确定缓存预算**，内存越大、生成越快——更大的 block cache / memtable、更大的计算块与 WriteBatch，意味着更少的 IPC 往返和 flush / compaction。

- **预算来源**：默认读取 `/proc/meminfo` 的 `MemAvailable`，取其约 **80%** 作为共享预算（下限 512 MiB；无法探测时退回 1 GiB 基线）。可用 `--mem-budget <MiB>` 显式指定。
- **预算拆分**（沿用原比例，随总量伸缩）：75% 共享 RocksDB block cache（其中含约 2/3 的 memtable）+ 12.5% 流水线预算 + 12.5% 余量。启动时打印实际数值。
- **参数自动适配**：`--chunk-size` / `--batch-size` 会按预算推导出的上限收敛——预算越大、单块越大（更少 IPC）；`--workers` 会收敛到 **CPU 核数**（SM3 是 CPU 密集型，进程多于核数只增开销）。被调整时打印 `[INFO] 根据缓存预算与 CPU 调整参数: ...`。
- 该预算是**缓存预算，不是进程 RSS 上限**：Python 运行时、compaction 临时分配与操作系统页缓存均不计入其中。
- 想预留更多内存给其他进程时，用 `--mem-budget` 把预算压低即可。

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
  --workers 30 \
  --shards 32
```

常用参数：

| 参数 | 说明 | 默认 |
| --- | --- | --- |
| `--prefix-file` | 号段前缀文件（生成模式必填） | — |
| `--db-root` | RocksDB 输出目录 | `/data/rocksdb/phone_sm3` |
| `--mem-budget` | 共享缓存预算（MiB）；`0` 表示按可用内存自动确定 | `0`（自动） |
| `--workers` | SM3 计算进程数（收敛到 CPU 核数） | 30 |
| `--shards` | 分片数（**必须是 1~256 范围内的 2 的幂**） | 32 |
| `--chunk-size` | 每个计算任务的手机号数量（受预算推导上限约束） | 500000 |
| `--batch-size` | RocksDB WriteBatch 大小（受预算推导上限约束） | 200000 |
| `--builder-workers` | 并行写入进程数（按 `shard % N` 分配 shard）；`>1` 时缓存预算在各进程间平分，`>shards` 会收敛到 shard 数 | 1 |
| `--tmp-root` | 临时目录，**仅 `--phase2-only` 使用** | `/data/rocksdb_build_tmp` |
| `--keep-temp` | **兼容保留**：流式生成不再创建临时文件，此开关无实际作用 | 关 |
| `--phase2-only` | 跳过计算，从 `--tmp-root` 导入既有的旧版 spool 文件（导入后保留源文件） | 关 |

> `--builder-workers` 默认为 `1`（单写入进程、完整共享预算）。仅当磁盘/IO 成为写入瓶颈时才建议调高——缓存会随之平分，务必结合 `--mem-budget` 评估每进程可用缓存。
> 流式生成不再产生 spool 文件；`--keep-temp` 仅为向后兼容而保留。`--phase2-only` 仅用于导入历史遗留的旧版 spool 文件，也支持 `--builder-workers` 并行导入。

`prefixes.txt` 每行一个号段前缀（如 `139`），支持 `#` 注释行；非 1~10 位 ASCII 数字的行会被跳过并告警，重复前缀会自动去重。

## 追加号段

`append.py` 将指定号段追加写入**既有**数据库，复用 `generate.py` 的同一套流式流水线、分片规则与缓存预算。由于 RocksDB 以 SM3 哈希为 key，追加等同于向对应 shard 批量 `put`：新号段直接写入，已存在的哈希会被幂等覆盖（value 仍是同一手机号），因此重复追加是安全的。

```bash
# 直接在命令行指定号段（可重复 --prefix）
python append.py \
  --db-root /path/to/rocksdb/phone_sm3 \
  --shards 32 \
  --prefix 166 --prefix 199

# 或从文件读取（与 --prefix 合并去重）
python append.py \
  --db-root /path/to/rocksdb/phone_sm3 \
  --shards 32 \
  --prefix-file new_prefixes.txt
```

| 参数 | 说明 | 默认 |
| --- | --- | --- |
| `--prefix` | 要追加的号段前缀，可重复指定；与 `--prefix-file` 合并去重 | — |
| `--prefix-file` | 号段前缀文件（格式同 `prefixes.txt`） | — |
| `--db-root` | 既有 RocksDB 目录；**默认必须已存在** | `/data/rocksdb/phone_sm3` |
| `--allow-create` | 允许在库不存在时新建（默认拒绝） | 关 |
| `--shards` | 分片数；默认自动识别既有库，显式指定时必须与原库相同 | 自动识别（新建时 32） |
| `--mem-budget` | 共享缓存预算（MiB）；`0` 表示按可用内存自动确定 | `0`（自动） |
| `--builder-workers` | 并行写入进程数（同 `generate.py`） | 1 |
| `--workers` / `--chunk-size` / `--batch-size` | 同 `generate.py`，受内存自适应预算与 CPU 约束 | 30 / 500000 / 200000 |

> 为防止原有数据"丢失"，`append.py` 会：
> - 在 `--db-root` 下找不到既有 `shard_XX` 时**直接报错**，而不是静默新建一个只含新号段的库（常见于路径写错或 Docker 未挂载数据卷）；确需新建请加 `--allow-create`；
> - 自动识别既有库的分片数，`--shards` 与之不一致时拒绝执行（分片数不同会让原有数据按新分片规则查不到）；
> - 追加前从每个 shard 抽样既有记录，追加后重新打开逐条校验，并打印前后记录数估计值。
>
> 查询服务以只读方式打开库，只能看到启动时的数据：**追加完成后需重启查询服务**，新号段才可查。`--prefix` 与 `--prefix-file` 可同时使用，至少需指定其一。

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

镜像内包含 `generate.py`、`append.py`、`server.py` 与 `prefixes.txt`，默认启动查询服务。`/data/rocksdb` 声明为数据卷，**生成、追加与查询都必须挂载同一个宿主机目录**，否则数据只写在容器内，容器删除后即丢失。

```bash
docker build -t sm3-phone .

# 1. 生成（数据写入宿主机 /path/to/rocksdb/phone_sm3）
docker run --rm -v /path/to/rocksdb:/data/rocksdb sm3-phone \
  python generate.py --prefix-file prefixes.txt --shards 32

# 2. 追加号段（先停止查询服务；挂载同一目录）
docker run --rm -v /path/to/rocksdb:/data/rocksdb sm3-phone \
  python append.py --prefix 166 --prefix 199

# 3. 启动 / 重启查询服务
docker run -d --name sm3-phone -p 8080:8080 \
  -v /path/to/rocksdb:/data/rocksdb \
  -e SHARDS=32 sm3-phone
```

环境变量 `DB_ROOT`（默认 `/data/rocksdb/phone_sm3`）与 `SHARDS`（默认 `32`）供查询服务使用；镜像内置基于 `/health` 的健康检查。

## 许可证

本项目以 [Apache License 2.0](LICENSE) 开源。详见 [LICENSE](LICENSE) 与 [NOTICE](NOTICE)。
