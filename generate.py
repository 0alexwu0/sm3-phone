import argparse
import hashlib
import multiprocessing as mp
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

from rocksdict import (
    BlockBasedOptions, Cache, Options, Rdict, WriteBatch,
    WriteBufferManager, WriteOptions,
)


DEFAULT_DB_ROOT = "/data/rocksdb/phone_sm3"
DEFAULT_TMP_ROOT = "/data/rocksdb_build_tmp"
DEFAULT_SHARDS = 32
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_BATCH_SIZE = 200_000
RECORD_SIZE = 43  # 32 bytes SM3 + 11 bytes phone
MIB = 1024 * 1024

# 所有分片共用预算，不能把 1 GiB 分别分配给每个 DB/worker。
# memtable 计入共享 block cache，不能重复相加。
# 768 MiB DB cache + 128 MiB 流水线预算 + 128 MiB 余量 = 1 GiB。
# 这是缓存预算，不是进程 RSS 上限；Python、compaction 和 OS 缓存另计。
CACHE_LIMIT = 1024 * MIB
ROCKSDB_CACHE_SIZE = 768 * MIB
MEMTABLE_LIMIT = 512 * MIB
PIPELINE_BUDGET = 128 * MIB
MAX_CHUNK_BYTES = MIB
MAX_BATCH_SIZE = 8192
# 为 worker 结果、序列化、IPC 和父进程结果的副本预留空间。
IPC_COPY_FACTOR = 4
MAX_WORKERS = PIPELINE_BUDGET // (MAX_CHUNK_BYTES * IPC_COPY_FACTOR)


def check_sm3():
    try:
        result = hashlib.new("sm3", b"13800138000").hexdigest()
    except ValueError as exc:
        raise RuntimeError("Python/OpenSSL 不支持 SM3") from exc
    print(f"[OK] OpenSSL SM3 supported: {result}")


def load_prefixes(filename):
    prefixes = []
    seen = set()
    with open(filename, "r", encoding="utf-8-sig") as source:
        for line in source:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if not line.isascii() or not line.isdigit() or len(line) >= 11:
                print(f"[WARN] 无效 prefix（需要 1~10 位 ASCII 数字）: {line}")
                continue
            if line not in seen:
                prefixes.append(line)
                seen.add(line)
    return prefixes


def create_tasks(prefixes, chunk_size):
    """惰性生成任务，不将全部号码或任务保存在内存中。"""
    for prefix in prefixes:
        suffix_width = 11 - len(prefix)
        count = 10 ** suffix_width
        for start in range(0, count, chunk_size):
            yield prefix, start, min(start + chunk_size, count), suffix_width


def hash_chunk(task):
    """只返回一个有界数据块，不写临时文件，也不打开 RocksDB。"""
    prefix, start, end, suffix_width = task
    data = bytearray((end - start) * RECORD_SIZE)
    prefix_bytes = prefix.encode("ascii")
    offset = 0
    for suffix in range(start, end):
        phone = prefix_bytes + str(suffix).zfill(suffix_width).encode("ascii")
        data[offset:offset + 32] = hashlib.new("sm3", phone).digest()
        data[offset + 32:offset + RECORD_SIZE] = phone
        offset += RECORD_SIZE
    return data


def create_rocksdb_options(cache, write_buffer_manager, shards):
    opts = Options(raw_mode=True)
    opts.create_if_missing(True)
    # 必须共用同一对象；缺少这些 API 时直接报错，不静默放弃限制。
    opts.set_write_buffer_manager(write_buffer_manager)
    table = BlockBasedOptions()
    table.set_block_cache(cache)
    table.set_cache_index_and_filter_blocks(True)
    table.set_pin_l0_filter_and_index_blocks_in_cache(False)
    table.set_pin_top_level_index_and_filter(False)
    opts.set_block_based_table_factory(table)
    memtable_size = min(8 * MIB, MEMTABLE_LIMIT // (shards * 2))
    opts.set_write_buffer_size(memtable_size)
    opts.set_max_write_buffer_number(2)
    opts.set_min_write_buffer_number_to_merge(1)
    opts.set_max_background_jobs(2)
    opts.set_max_open_files(64)
    opts.set_target_file_size_base(64 * MIB)
    opts.set_level_compaction_dynamic_level_bytes(True)
    opts.set_bytes_per_sync(MIB)
    opts.set_stats_dump_period_sec(0)
    # 保留正常 compaction/stall 阈值，磁盘跟不上时产生背压。
    return opts


class ShardWriter:
    """单一写入进程拥有所有分片，使共享缓存预算真正生效。"""

    def __init__(self, db_root, shards, batch_size):
        self.db_root = Path(db_root)
        self.shards = shards
        self.batch_size = batch_size
        self.databases = {}
        self.cache = Cache(ROCKSDB_CACHE_SIZE)
        self.write_buffer_manager = (
            WriteBufferManager.new_write_buffer_manager_with_cache(
                MEMTABLE_LIMIT, True, self.cache
            )
        )
        self.write_options = WriteOptions()
        self.write_options.sync = False
        # 保留 WAL，已提交批次可以在进程异常退出后恢复。
        disable_wal = self.write_options.disable_wal
        if callable(disable_wal):
            disable_wal(False)
        else:
            self.write_options.disable_wal = False

    def get_db(self, shard):
        if shard not in self.databases:
            options = create_rocksdb_options(
                self.cache, self.write_buffer_manager, self.shards
            )
            path = self.db_root / f"shard_{shard:02d}"
            path.mkdir(parents=True, exist_ok=True)
            self.databases[shard] = Rdict(str(path), options)
        return self.databases[shard]

    def write_records(self, data, expected_shard=None):
        if len(data) % RECORD_SIZE:
            raise RuntimeError("Corrupted record block: size is not a multiple of 43")
        # 批次最多包含当前数据块的记录，不随总数据量增长。
        batches = {}
        counts = {}
        for offset in range(0, len(data), RECORD_SIZE):
            shard = data[offset] & (self.shards - 1)
            if expected_shard is not None and shard != expected_shard:
                raise RuntimeError(f"Spool record does not belong to shard_{expected_shard:02d}")
            if shard not in batches:
                batches[shard] = WriteBatch(raw_mode=True)
                counts[shard] = 0
            batches[shard].put(
                bytes(data[offset:offset + 32]),
                bytes(data[offset + 32:offset + RECORD_SIZE]),
            )
            counts[shard] += 1
            if counts[shard] >= self.batch_size:
                self.get_db(shard).write(batches.pop(shard), self.write_options)
                del counts[shard]
        # 尾批立即提交，不等所有号码生成完后才写入。
        for shard, batch in batches.items():
            self.get_db(shard).write(batch, self.write_options)
        return len(data) // RECORD_SIZE

    def close(self):
        # 即使其中一个分片失败，也尝试关闭其余分片。
        first_error = None
        for db in self.databases.values():
            try:
                db.flush(wait=True)
                db.flush_wal(sync=True)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
            finally:
                try:
                    db.close()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
        self.databases.clear()
        if first_error is not None:
            raise first_error


def stream_generate(prefixes, workers, shards, chunk_size, batch_size, db_root):
    total = sum(10 ** (11 - len(prefix)) for prefix in prefixes)
    tasks = iter(create_tasks(prefixes, chunk_size))
    worker_count = min(workers, sum(
        (10 ** (11 - len(prefix)) + chunk_size - 1) // chunk_size
        for prefix in prefixes
    ))
    writer = ShardWriter(db_root, shards, batch_size)
    processed = 0
    begin = time.monotonic()
    try:
        # spawn 同时支持 Linux/Windows，不继承已打开的 DB/线程。
        with ProcessPoolExecutor(
            max_workers=worker_count, mp_context=mp.get_context("spawn")
        ) as pool:
            pending = set()

            def fill_pending():
                while len(pending) < worker_count:
                    task = next(tasks, None)
                    if task is None:
                        break
                    pending.add(pool.submit(hash_chunk, task))

            fill_pending()
            while pending:
                completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                # 完整消费并释放结果后才补充任务，避免结果无界堆积。
                while completed:
                    future = completed.pop()
                    data = future.result()
                    processed += writer.write_records(data)
                    del data, future
                fill_pending()
                elapsed = max(time.monotonic() - begin, 1e-9)
                print(
                    f"\r[WRITE] {processed:,}/{total:,} "
                    f"{processed / total * 100:6.2f}% "
                    f"{processed / elapsed:,.0f} phone/s",
                    end="", flush=True,
                )
        if processed != total:
            raise RuntimeError(f"Record count mismatch: {processed} != {total}")
    finally:
        print()
        writer.close()
    print(f"[DONE] {processed:,} records, {time.monotonic() - begin:.1f}s")
    return processed


def import_spool(tmp_root, shards, batch_size, db_root):
    """兼容旧 --phase2-only；逐块导入已有文件，保留源文件。"""
    root = Path(tmp_root)
    if not root.is_dir():
        raise ValueError(f"Temporary directory does not exist: {root}")
    writer = ShardWriter(db_root, shards, batch_size)
    records = 0
    files = 0
    try:
        for worker_dir in root.iterdir():
            if not worker_dir.is_dir():
                continue
            for path in worker_dir.glob("shard_*.bin"):
                shard = int(path.stem[len("shard_"):])
                if not 0 <= shard < shards:
                    raise ValueError(f"Shard outside --shards range: {path}")
                if path.stat().st_size % RECORD_SIZE:
                    raise RuntimeError(f"Corrupted spool file: {path}")
                with path.open("rb", buffering=0) as source:
                    while True:
                        data = source.read((MAX_CHUNK_BYTES // RECORD_SIZE) * RECORD_SIZE)
                        if not data:
                            break
                        records += writer.write_records(data, expected_shard=shard)
                files += 1
                print(f"[IMPORT] {path.name}: total={records:,}")
        if files == 0:
            raise RuntimeError(f"No spool files found in {root}")
    finally:
        writer.close()
    print(f"[DONE] imported {records:,} records; source files retained")
    return records


def main():
    parser = argparse.ArgumentParser(description="Streaming SM3 Phone RocksDB Builder (1 GiB cache budget)")
    parser.add_argument("--prefix-file")
    parser.add_argument("--db-root", default=DEFAULT_DB_ROOT)
    parser.add_argument("--tmp-root", default=DEFAULT_TMP_ROOT,
                        help="Only used by --phase2-only")
    parser.add_argument("--workers", type=int, default=30)
    parser.add_argument("--builder-workers", type=int, default=None,
                        help="Deprecated: all shards now share one writer/cache")
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARDS)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--keep-temp", action="store_true",
                        help="Compatibility flag; streaming does not create spool files")
    parser.add_argument("--phase2-only", action="store_true",
                        help="Import existing legacy spool files and retain them")
    args = parser.parse_args()
    if args.shards <= 0 or args.shards > 256 or args.shards & (args.shards - 1):
        parser.error("--shards 必须是 1~256 范围内的 2 的幂")
    for name in ("workers", "chunk_size", "batch_size"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} 必须大于 0")
    if not args.phase2_only and not args.prefix_file:
        parser.error("生成模式必须指定 --prefix-file")

    workers = min(args.workers, MAX_WORKERS)
    chunk_size = min(args.chunk_size, MAX_CHUNK_BYTES // RECORD_SIZE)
    batch_size = min(args.batch_size, MAX_BATCH_SIZE)
    if (workers, chunk_size, batch_size) != (args.workers, args.chunk_size, args.batch_size):
        print(f"[INFO] 根据缓存预算限制参数: workers={workers}, "
              f"chunk-size={chunk_size}, batch-size={batch_size}")
    if args.builder_workers is not None:
        print("[INFO] --builder-workers 已弃用；所有分片由同一进程写入以共享缓存")
    print(f"[INFO] Cache budget: {CACHE_LIMIT // MIB} MiB total; "
          f"{ROCKSDB_CACHE_SIZE // MIB} MiB shared RocksDB cache "
          f"(includes up to {MEMTABLE_LIMIT // MIB} MiB memtables); "
          f"{PIPELINE_BUDGET // MIB} MiB pipeline reserve; 128 MiB headroom")
    print("[INFO] Cache budget excludes process runtime, compaction allocations and OS page cache")

    if args.phase2_only:
        import_spool(args.tmp_root, args.shards, batch_size, args.db_root)
    else:
        check_sm3()
        prefixes = load_prefixes(args.prefix_file)
        if not prefixes:
            raise RuntimeError("prefix 文件中没有有效前缀")
        stream_generate(prefixes, workers, args.shards, chunk_size, batch_size, args.db_root)
    print("BUILD COMPLETED")


if __name__ == "__main__":
    mp.freeze_support()
    main()
