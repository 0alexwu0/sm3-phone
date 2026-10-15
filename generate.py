import argparse
import hashlib
import multiprocessing as mp
import os
import shutil
import sys
import time
from pathlib import Path

from rocksdict import (
    Rdict,
    Options,
    WriteBatch,
    WriteOptions,
)

# ============================================================
# 基本配置
# ============================================================

DEFAULT_DB_ROOT = "/data/rocksdb/phone_sm3"
DEFAULT_TMP_ROOT = "/data/rocksdb_build_tmp"

# 必须为 2 的幂
DEFAULT_SHARDS = 32

# SM3 = 32 bytes
# Phone = 11 bytes
RECORD_SIZE = 43

# 每个计算任务生成多少手机号
DEFAULT_CHUNK_SIZE = 500_000

# 每个 RocksDB WriteBatch 数量
DEFAULT_BATCH_SIZE = 200_000

# worker 临时文件缓存
# 每个 shard 到达该大小再写磁盘
SPOOL_BUFFER_SIZE = 2 * 1024 * 1024


# ============================================================
# 全局 worker 变量
# ============================================================

_worker_tmp_dir = None
_worker_files = None
_worker_buffers = None
_worker_shards = None


# ============================================================
# 检测 SM3
# ============================================================

def check_sm3():
    try:
        result = hashlib.new(
            "sm3",
            b"13800138000"
        ).hexdigest()

        print(
            f"[OK] OpenSSL SM3 supported: {result}"
        )

    except Exception as e:
        print(
            "[ERROR] Python/OpenSSL 不支持 SM3"
        )
        print(e)
        sys.exit(1)


# ============================================================
# 临时文件 worker 初始化
# ============================================================

def init_hash_worker(
    tmp_root,
    shards
):
    global _worker_tmp_dir
    global _worker_files
    global _worker_buffers
    global _worker_shards

    pid = os.getpid()

    _worker_shards = shards

    _worker_tmp_dir = os.path.join(
        tmp_root,
        f"worker_{pid}"
    )

    os.makedirs(
        _worker_tmp_dir,
        exist_ok=True
    )

    _worker_files = [None] * shards

    _worker_buffers = [
        bytearray()
        for _ in range(shards)
    ]


# ============================================================
# flush worker 本地缓存
# ============================================================

def flush_worker_buffer(shard):
    global _worker_files
    global _worker_buffers
    global _worker_tmp_dir

    buf = _worker_buffers[shard]

    if not buf:
        return

    if _worker_files[shard] is None:

        filename = os.path.join(
            _worker_tmp_dir,
            f"shard_{shard:02d}.bin"
        )

        _worker_files[shard] = open(
            filename,
            "ab",
            buffering=8 * 1024 * 1024
        )

    _worker_files[shard].write(buf)

    buf.clear()


# ============================================================
# SM3 批量计算
# ============================================================

def hash_chunk(task):

    prefix, start, end, suffix_width = task

    global _worker_buffers
    global _worker_shards

    sm3_new = hashlib.new

    prefix_bytes = prefix.encode("ascii")

    count = 0

    for suffix in range(start, end):

        suffix_bytes = str(
            suffix
        ).zfill(
            suffix_width
        ).encode("ascii")

        phone = (
            prefix_bytes +
            suffix_bytes
        )

        digest = sm3_new(
            "sm3",
            phone
        ).digest()

        # 32 shards 时:
        # digest[0] & 31
        shard = (
            digest[0] &
            (_worker_shards - 1)
        )

        buf = _worker_buffers[shard]

        # 固定长度
        # 32 bytes SM3
        # 11 bytes phone
        buf.extend(digest)
        buf.extend(phone)

        if len(buf) >= SPOOL_BUFFER_SIZE:
            flush_worker_buffer(shard)

        count += 1

    # 当前任务完成后 flush
    for shard in range(_worker_shards):
        flush_worker_buffer(shard)

    return count


# ============================================================
# prefix 读取
# ============================================================

def load_prefixes(filename):

    prefixes = []

    with open(
        filename,
        "r",
        encoding="utf-8"
    ) as f:

        for line in f:

            line = line.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            if not line.isdigit():
                print(
                    f"[WARN] 非数字 prefix: {line}"
                )
                continue

            if len(line) >= 11:
                print(
                    f"[WARN] prefix 过长: {line}"
                )
                continue

            prefixes.append(line)

    return prefixes


# ============================================================
# 生成所有 task
# ============================================================

def create_tasks(
    prefixes,
    chunk_size
):

    tasks = []

    total = 0

    for prefix in prefixes:

        suffix_width = (
            11 - len(prefix)
        )

        count = (
            10 ** suffix_width
        )

        total += count

        for start in range(
            0,
            count,
            chunk_size
        ):

            end = min(
                start + chunk_size,
                count
            )

            tasks.append(
                (
                    prefix,
                    start,
                    end,
                    suffix_width
                )
            )

    return tasks, total


# ============================================================
# 第一阶段
# ============================================================

def phase1_hash(
    prefixes,
    workers,
    shards,
    chunk_size,
    tmp_root
):

    print()
    print(
        "========================================"
    )
    print(
        " PHASE 1 - SM3 CALCULATION"
    )
    print(
        "========================================"
    )

    tasks, total = create_tasks(
        prefixes,
        chunk_size
    )

    print(
        f"Workers       : {workers}"
    )
    print(
        f"Shards        : {shards}"
    )
    print(
        f"Chunk Size    : {chunk_size:,}"
    )
    print(
        f"Tasks         : {len(tasks):,}"
    )
    print(
        f"Total Phones  : {total:,}"
    )

    processed = 0

    begin = time.time()

    ctx = mp.get_context("fork")

    with ctx.Pool(
        processes=workers,
        initializer=init_hash_worker,
        initargs=(
            tmp_root,
            shards
        )
    ) as pool:

        for count in pool.imap_unordered(
            hash_chunk,
            tasks,
            chunksize=1
        ):

            processed += count

            elapsed = (
                time.time() - begin
            )

            speed = (
                processed / elapsed
                if elapsed > 0
                else 0
            )

            percent = (
                processed /
                total *
                100
            )

            print(
                f"\r"
                f"[HASH] "
                f"{processed:,}/"
                f"{total:,} "
                f"{percent:6.2f}% "
                f"{speed:,.0f} phone/s",
                end="",
                flush=True
            )

    elapsed = (
        time.time() - begin
    )

    print()

    print(
        f"[PHASE1 DONE] "
        f"{processed:,} phones"
    )

    print(
        f"[PHASE1 TIME] "
        f"{elapsed:.1f}s"
    )

    print(
        f"[PHASE1 AVG] "
        f"{processed / elapsed:,.0f} phone/s"
    )


# ============================================================
# RocksDB 参数
# ============================================================

def create_rocksdb_options():

    opts = Options(
        raw_mode=True
    )

    opts.create_if_missing(True)

    # 不强制 fsync
    if hasattr(
        opts,
        "set_use_fsync"
    ):
        opts.set_use_fsync(False)

    # 一个 shard 一个写进程。
    # 不需要每个 DB 再启动 32 个后台线程。
    if hasattr(
        opts,
        "increase_parallelism"
    ):
        opts.increase_parallelism(2)

    # --------------------------------------------------------
    # MemTable
    #
    # 每 shard:
    # 512 MB * 最多 4 个
    #
    # 但不会所有 shard 同时跑满。
    # --------------------------------------------------------

    if hasattr(
        opts,
        "set_write_buffer_size"
    ):
        opts.set_write_buffer_size(
            512 * 1024 * 1024
        )

    if hasattr(
        opts,
        "set_max_write_buffer_number"
    ):
        opts.set_max_write_buffer_number(
            4
        )

    if hasattr(
        opts,
        "set_min_write_buffer_number_to_merge"
    ):
        opts.set_min_write_buffer_number_to_merge(
            2
        )

    # SST 文件大一点
    if hasattr(
        opts,
        "set_target_file_size_base"
    ):
        opts.set_target_file_size_base(
            512 * 1024 * 1024
        )

    # bulk build 阶段尽量不因为 L0 stall
    if hasattr(
        opts,
        "set_level_zero_slowdown_writes_trigger"
    ):
        opts.set_level_zero_slowdown_writes_trigger(
            1000
        )

    if hasattr(
        opts,
        "set_level_zero_stop_writes_trigger"
    ):
        opts.set_level_zero_stop_writes_trigger(
            2000
        )

    # 大数据集动态 level
    if hasattr(
        opts,
        "set_level_compaction_dynamic_level_bytes"
    ):
        opts.set_level_compaction_dynamic_level_bytes(
            True
        )

    # 大文件顺序写
    if hasattr(
        opts,
        "set_bytes_per_sync"
    ):
        opts.set_bytes_per_sync(
            16 * 1024 * 1024
        )

    # 避免统计日志带来额外成本
    if hasattr(
        opts,
        "set_stats_dump_period_sec"
    ):
        opts.set_stats_dump_period_sec(
            0
        )

    return opts


# ============================================================
# WriteOptions
# ============================================================

def configure_write_options(db):

    try:

        wopt = WriteOptions()

        # RocksDB 本身默认 sync=False。
        #
        # 部分 rocksdict 版本没有 set_sync，
        # 因此这里做版本兼容。

        if hasattr(
            wopt,
            "set_sync"
        ):
            wopt.set_sync(False)

        # bulk build 数据可重新生成
        # 关闭 WAL
        if hasattr(
            wopt,
            "disable_wal"
        ):
            wopt.disable_wal(True)

            print(
                "[INFO] WAL disabled"
            )

        else:

            print(
                "[WARN] 当前 rocksdict "
                "没有 disable_wal API"
            )

        if hasattr(
            db,
            "set_write_options"
        ):
            db.set_write_options(
                wopt
            )

    except Exception as e:

        print(
            f"[WARN] WriteOptions: {e}"
        )


# ============================================================
# 查找 shard 临时文件
# ============================================================

def find_shard_files(
    tmp_root,
    shard
):

    result = []

    filename = (
        f"shard_{shard:02d}.bin"
    )

    for entry in os.scandir(
        tmp_root
    ):

        if not entry.is_dir():
            continue

        path = os.path.join(
            entry.path,
            filename
        )

        if os.path.exists(path):
            result.append(path)

    return result


# ============================================================
# build 单个 shard
# ============================================================

def build_shard(args):

    (
        shard,
        db_root,
        tmp_root,
        batch_size
    ) = args

    db_path = os.path.join(
        db_root,
        f"shard_{shard:02d}"
    )

    os.makedirs(
        db_path,
        exist_ok=True
    )

    opts = create_rocksdb_options()

    db = Rdict(
        db_path,
        opts
    )

    configure_write_options(
        db
    )

    files = find_shard_files(
        tmp_root,
        shard
    )

    total_count = 0

    # 一次读若干完整 record
    read_records = max(
        batch_size,
        100_000
    )

    read_size = (
        read_records *
        RECORD_SIZE
    )

    for filename in files:

        with open(
            filename,
            "rb",
            buffering=16 * 1024 * 1024
        ) as f:

            while True:

                data = f.read(
                    read_size
                )

                if not data:
                    break

                if (
                    len(data) %
                    RECORD_SIZE
                    != 0
                ):
                    raise RuntimeError(
                        f"Corrupted spool file: "
                        f"{filename}"
                    )

                view = memoryview(data)

                records = (
                    len(data) //
                    RECORD_SIZE
                )

                batch = WriteBatch(
                    raw_mode=True
                )

                offset = 0

                for _ in range(records):

                    key = bytes(
                        view[
                            offset:
                            offset + 32
                        ]
                    )

                    value = bytes(
                        view[
                            offset + 32:
                            offset + 43
                        ]
                    )

                    batch.put(
                        key,
                        value
                    )

                    offset += RECORD_SIZE

                db.write(
                    batch
                )

                total_count += records

    db.close()

    return (
        shard,
        total_count
    )


# ============================================================
# 第二阶段
# ============================================================

def phase2_build(
    shards,
    db_root,
    tmp_root,
    builder_workers,
    batch_size
):

    print()
    print(
        "========================================"
    )
    print(
        " PHASE 2 - BUILD ROCKSDB SHARDS"
    )
    print(
        "========================================"
    )

    print(
        f"Builder Workers : "
        f"{builder_workers}"
    )

    print(
        f"Batch Size      : "
        f"{batch_size:,}"
    )

    jobs = [
        (
            shard,
            db_root,
            tmp_root,
            batch_size
        )
        for shard
        in range(shards)
    ]

    begin = time.time()

    finished = 0
    total_records = 0

    ctx = mp.get_context("fork")

    with ctx.Pool(
        processes=builder_workers
    ) as pool:

        for (
            shard,
            count
        ) in pool.imap_unordered(
            build_shard,
            jobs
        ):

            finished += 1
            total_records += count

            print(
                f"[DB] "
                f"shard_{shard:02d} "
                f"done "
                f"records={count:,} "
                f"({finished}/{shards})"
            )

    elapsed = (
        time.time() - begin
    )

    print()

    print(
        f"[PHASE2 DONE] "
        f"{total_records:,} records"
    )

    print(
        f"[PHASE2 TIME] "
        f"{elapsed:.1f}s"
    )


# ============================================================
# 验证临时文件
# ============================================================

def validate_spool(
    tmp_root
):

    total_bytes = 0
    files = 0

    for root, dirs, filenames in os.walk(
        tmp_root
    ):

        for filename in filenames:

            if not filename.endswith(
                ".bin"
            ):
                continue

            path = os.path.join(
                root,
                filename
            )

            size = os.path.getsize(
                path
            )

            if size % RECORD_SIZE != 0:

                raise RuntimeError(
                    f"Invalid spool file: "
                    f"{path}"
                )

            total_bytes += size
            files += 1

    records = (
        total_bytes //
        RECORD_SIZE
    )

    print(
        f"[VALIDATE] files   : "
        f"{files:,}"
    )

    print(
        f"[VALIDATE] size    : "
        f"{total_bytes / 1024**3:.2f} GiB"
    )

    print(
        f"[VALIDATE] records : "
        f"{records:,}"
    )

    return records


# ============================================================
# main
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "High Performance "
            "SM3 Phone RocksDB Builder"
        )
    )

    parser.add_argument(
        "--prefix-file",
        required=True
    )

    parser.add_argument(
        "--db-root",
        default=DEFAULT_DB_ROOT
    )

    parser.add_argument(
        "--tmp-root",
        default=DEFAULT_TMP_ROOT
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=30
    )

    parser.add_argument(
        "--builder-workers",
        type=int,
        default=8
    )

    parser.add_argument(
        "--shards",
        type=int,
        default=DEFAULT_SHARDS
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE
    )

    parser.add_argument(
        "--keep-temp",
        action="store_true"
    )

    parser.add_argument(
        "--phase2-only",
        action="store_true"
    )

    args = parser.parse_args()

    # shards 必须是 2 次幂
    if (
        args.shards <= 0
        or
        args.shards &
        (
            args.shards - 1
        )
    ):
        raise ValueError(
            "--shards 必须是 2 的幂"
        )

    check_sm3()

    prefixes = load_prefixes(
        args.prefix_file
    )

    if not prefixes:
        raise RuntimeError(
            "prefix 文件为空"
        )

    print()
    print(
        "========================================"
    )
    print(
        " SM3 Phone Database Generator"
    )
    print(
        "========================================"
    )

    print(
        f"CPU Count       : "
        f"{os.cpu_count()}"
    )

    print(
        f"Hash Workers    : "
        f"{args.workers}"
    )

    print(
        f"Builder Workers : "
        f"{args.builder_workers}"
    )

    print(
        f"Shards          : "
        f"{args.shards}"
    )

    print(
        f"DB Root         : "
        f"{args.db_root}"
    )

    print(
        f"Temp Root       : "
        f"{args.tmp_root}"
    )

    os.makedirs(
        args.db_root,
        exist_ok=True
    )

    if not args.phase2_only:

        if os.path.exists(
            args.tmp_root
        ):
            print(
                f"[INFO] remove old tmp: "
                f"{args.tmp_root}"
            )

            shutil.rmtree(
                args.tmp_root
            )

        os.makedirs(
            args.tmp_root,
            exist_ok=True
        )

        phase1_hash(
            prefixes=prefixes,
            workers=args.workers,
            shards=args.shards,
            chunk_size=args.chunk_size,
            tmp_root=args.tmp_root
        )

    validate_spool(
        args.tmp_root
    )

    phase2_build(
        shards=args.shards,
        db_root=args.db_root,
        tmp_root=args.tmp_root,
        builder_workers=args.builder_workers,
        batch_size=args.batch_size
    )

    if not args.keep_temp:

        print(
            "[INFO] Removing temporary files..."
        )

        shutil.rmtree(
            args.tmp_root
        )

    print()
    print(
        "========================================"
    )
    print(
        " BUILD COMPLETED"
    )
    print(
        "========================================"
    )


if __name__ == "__main__":

    mp.freeze_support()

    main()