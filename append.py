"""追加号段到既有库。

复用 generate.py 的流式流水线，把指定号段写入 **既有** RocksDB 库。
RocksDB 以 SM3 哈希为 key，追加等同于向现有 shard 批量 put：
- 新号段的记录直接写入对应 shard；
- 已存在的哈希会被幂等覆盖（value 仍是同一手机号），重复追加安全。

号段可来自命令行 `--prefix`（可重复）和/或 `--prefix-file`，二者合并去重。
分片规则、缓存预算、参数自动下调均与 generate.py 完全一致，
因此追加时 **必须** 使用与原库相同的 `--shards`。
"""
import argparse
from pathlib import Path

from generate import (
    MIB, check_sm3, clamp_params, load_prefixes, plan_budget,
    print_budget, resolve_builders, resolve_workers, stream_generate,
)

DEFAULT_DB_ROOT = "/data/rocksdb/phone_sm3"
DEFAULT_SHARDS = 32
DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_BATCH_SIZE = 200_000
DEFAULT_WORKERS = 30


def valid_prefix(prefix):
    """与 generate.load_prefixes 相同的规则：1~10 位 ASCII 数字。"""
    return prefix.isascii() and prefix.isdigit() and 1 <= len(prefix) <= 10


def collect_prefixes(cli_prefixes, prefix_file):
    """合并命令行与文件中的号段，按出现顺序去重并校验。"""
    prefixes = []
    seen = set()
    for prefix in cli_prefixes or []:
        prefix = prefix.strip()
        if not valid_prefix(prefix):
            print(f"[WARN] 无效 prefix（需要 1~10 位 ASCII 数字）: {prefix}")
            continue
        if prefix not in seen:
            prefixes.append(prefix)
            seen.add(prefix)
    if prefix_file:
        for prefix in load_prefixes(prefix_file):
            if prefix not in seen:
                prefixes.append(prefix)
                seen.add(prefix)
    return prefixes


def main():
    parser = argparse.ArgumentParser(
        description="追加指定号段到既有 SM3 Phone RocksDB 库（内存自适应缓存预算）"
    )
    parser.add_argument("--prefix", action="append", metavar="PREFIX",
                        help="要追加的号段前缀，可重复指定（如 --prefix 166 --prefix 199）")
    parser.add_argument("--prefix-file",
                        help="号段前缀文件（每行一个，与 --prefix 合并去重）")
    parser.add_argument("--db-root", default=DEFAULT_DB_ROOT,
                        help="既有 RocksDB 目录；不存在则新建")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--mem-budget", type=int, default=0, metavar="MIB",
                        help="共享缓存预算（MiB）；0 表示按可用内存自动确定")
    parser.add_argument("--builder-workers", type=int, default=1,
                        help="并行写入进程数（按 shard 分配）；>1 时缓存预算在各进程间平分")
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARDS,
                        help="必须与原库相同（1~256 范围内的 2 的幂）")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    if args.shards <= 0 or args.shards > 256 or args.shards & (args.shards - 1):
        parser.error("--shards 必须是 1~256 范围内的 2 的幂")
    for name in ("workers", "chunk_size", "batch_size", "builder_workers"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} 必须大于 0")
    if args.mem_budget < 0:
        parser.error("--mem-budget 不能为负")
    if not args.prefix and not args.prefix_file:
        parser.error("必须通过 --prefix 和/或 --prefix-file 指定要追加的号段")

    budget, source = plan_budget(args.mem_budget)
    workers = resolve_workers(args.workers)
    builders = resolve_builders(args.builder_workers, args.shards)
    chunk_size, batch_size = clamp_params(budget, workers, args.chunk_size, args.batch_size)
    if (workers, chunk_size, batch_size) != (args.workers, args.chunk_size, args.batch_size):
        print(f"[INFO] 根据缓存预算与 CPU 调整参数: workers={workers}, "
              f"chunk-size={chunk_size}, batch-size={batch_size}")
    if builders != args.builder_workers:
        print(f"[INFO] builder-workers 收敛到 shard 数: builder-workers={builders}")
    print_budget(budget, source)
    if builders > 1:
        print(f"[INFO] {builders} 个并行写入进程，每个约 "
              f"{budget.cache_size // builders // MIB} MiB cache（预算按 builder 平分）")

    db_root = Path(args.db_root)
    if db_root.exists():
        print(f"[INFO] 追加到既有库: {db_root}")
    else:
        print(f"[INFO] 库不存在，将新建: {db_root}")

    check_sm3()
    prefixes = collect_prefixes(args.prefix, args.prefix_file)
    if not prefixes:
        raise RuntimeError("没有有效的号段可追加")
    print(f"[INFO] 待追加号段 ({len(prefixes)}): {', '.join(prefixes)}")

    stream_generate(prefixes, workers, args.shards, chunk_size, batch_size,
                    args.db_root, budget, builders)
    print("APPEND COMPLETED")


if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support()
    main()
