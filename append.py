"""追加号段到既有库。

复用 generate.py 的流式流水线，把指定号段写入 **既有** RocksDB 库。
RocksDB 以 SM3 哈希为 key，追加等同于向现有 shard 批量 put：
- 新号段的记录直接写入对应 shard；
- 已存在的哈希会被幂等覆盖（value 仍是同一手机号），重复追加安全。

号段可来自命令行 `--prefix`（可重复）和/或 `--prefix-file`，二者合并去重。
分片规则、缓存预算、参数自动下调均与 generate.py 完全一致。

防止既有数据"丢失"的保护措施：
- 默认要求 `--db-root` 指向一个已存在的库；路径写错 / Docker 未挂载数据卷时
  直接报错，而不是静默新建一个只含新号段的空库（需新建请显式加 `--allow-create`）；
- 自动从既有 `shard_XX` 目录识别分片数，与 `--shards` 不一致时拒绝执行，
  避免记录被写入错误分片、原有数据按新分片数查不到；
- 追加前从每个 shard 抽样既有记录，追加后只读重新打开逐条校验，确认原数据完好。

注意：查询服务以只读方式打开库，只能看到打开那一刻的数据。
追加期间请停止查询服务，追加完成后再启动（或重启），新号段才可见。
"""
import argparse
import itertools
import re
from pathlib import Path

from rocksdict import AccessType, Options, Rdict

from generate import (
    MIB, check_sm3, clamp_params, load_prefixes, plan_budget,
    print_budget, resolve_builders, resolve_workers, stream_generate,
)

DEFAULT_DB_ROOT = "/data/rocksdb/phone_sm3"
DEFAULT_SHARDS = 32
SAMPLES_PER_SHARD = 64
SHARD_DIR_RE = re.compile(r"^shard_(\d+)$")
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


def is_power_of_two(value):
    return 0 < value <= 256 and not value & (value - 1)


def detect_shards(db_root):
    """识别既有库的分片布局；返回 shard 数，库不存在/为空时返回 0。"""
    if not db_root.exists():
        return 0
    if not db_root.is_dir():
        raise RuntimeError(f"--db-root 不是目录: {db_root}")
    ids = []
    for entry in db_root.iterdir():
        match = SHARD_DIR_RE.match(entry.name)
        if match and entry.is_dir():
            ids.append(int(match.group(1)))
    if not ids:
        return 0
    ids.sort()
    count = len(ids)
    if ids != list(range(count)) or not is_power_of_two(count):
        raise RuntimeError(
            f"既有库分片目录不完整或数量不是 2 的幂: {db_root} "
            f"(发现 {count} 个 shard，编号 {ids[0]}~{ids[-1]})"
        )
    missing = [i for i in ids if not (db_root / f"shard_{i:02d}" / "CURRENT").is_file()]
    if missing:
        raise RuntimeError(
            f"以下 shard 不是有效的 RocksDB（缺少 CURRENT）: "
            f"{', '.join(f'shard_{i:02d}' for i in missing)}"
        )
    return count


def open_read_only(path):
    return Rdict(str(path), options=Options(raw_mode=True),
                 access_type=AccessType.read_only())


def sample_existing(db_root, shards, per_shard=SAMPLES_PER_SHARD):
    """从每个 shard 抽取前若干条记录，并读取估计 key 数，用于追加后校验。"""
    samples = {}
    estimates = {}
    for shard in range(shards):
        db = open_read_only(db_root / f"shard_{shard:02d}")
        try:
            samples[shard] = list(itertools.islice(db.items(), per_shard))
            estimates[shard] = db.property_int_value("rocksdb.estimate-num-keys") or 0
        finally:
            db.close()
    return samples, estimates


def verify_existing(db_root, samples):
    """追加完成后只读重新打开，确认抽样的既有记录全部仍在且值未变。"""
    checked = 0
    lost = []
    for shard, items in samples.items():
        if not items:
            continue
        db = open_read_only(db_root / f"shard_{shard:02d}")
        try:
            for key, value in items:
                checked += 1
                if db.get(key) != value:
                    lost.append((shard, key.hex()))
        finally:
            db.close()
    if lost:
        shard, key = lost[0]
        raise RuntimeError(
            f"追加后校验失败: {len(lost)}/{checked} 条既有抽样记录缺失或被改写 "
            f"(例: shard_{shard:02d} key={key})"
        )
    return checked


def main():
    parser = argparse.ArgumentParser(
        description="追加指定号段到既有 SM3 Phone RocksDB 库（内存自适应缓存预算）"
    )
    parser.add_argument("--prefix", action="append", metavar="PREFIX",
                        help="要追加的号段前缀，可重复指定（如 --prefix 166 --prefix 199）")
    parser.add_argument("--prefix-file",
                        help="号段前缀文件（每行一个，与 --prefix 合并去重）")
    parser.add_argument("--db-root", default=DEFAULT_DB_ROOT,
                        help="既有 RocksDB 目录（必须已存在，除非指定 --allow-create）")
    parser.add_argument("--allow-create", action="store_true",
                        help="允许在库不存在时新建（默认拒绝，防止路径/挂载错误导致写入空库）")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--mem-budget", type=int, default=0, metavar="MIB",
                        help="共享缓存预算（MiB）；0 表示按可用内存自动确定")
    parser.add_argument("--builder-workers", type=int, default=1,
                        help="并行写入进程数（按 shard 分配）；>1 时缓存预算在各进程间平分")
    parser.add_argument("--shards", type=int, default=None,
                        help="默认自动识别既有库的分片数；显式指定时必须与原库相同")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    args = parser.parse_args()

    if args.shards is not None and not is_power_of_two(args.shards):
        parser.error("--shards 必须是 1~256 范围内的 2 的幂")
    for name in ("workers", "chunk_size", "batch_size", "builder_workers"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} 必须大于 0")
    if args.mem_budget < 0:
        parser.error("--mem-budget 不能为负")
    if not args.prefix and not args.prefix_file:
        parser.error("必须通过 --prefix 和/或 --prefix-file 指定要追加的号段")

    # 先校验目标库，再做任何耗时/写入操作。
    db_root = Path(args.db_root)
    existing_shards = detect_shards(db_root)
    if existing_shards:
        if args.shards is not None and args.shards != existing_shards:
            parser.error(
                f"--shards={args.shards} 与既有库的分片数 {existing_shards} 不一致；"
                f"分片数不同会把记录写入错误分片，导致原有数据查不到"
            )
        args.shards = existing_shards
        print(f"[INFO] 追加到既有库: {db_root}（{existing_shards} 个 shard）")
    else:
        if not args.allow_create:
            parser.error(
                f"未在 {db_root} 找到既有库（没有 shard_XX 目录）。"
                f"请检查 --db-root 路径或 Docker 数据卷挂载；"
                f"确需新建请加 --allow-create"
            )
        if args.shards is None:
            args.shards = DEFAULT_SHARDS
        print(f"[INFO] 库不存在，将新建: {db_root}（{args.shards} 个 shard）")

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

    check_sm3()
    prefixes = collect_prefixes(args.prefix, args.prefix_file)
    if not prefixes:
        raise RuntimeError("没有有效的号段可追加")
    print(f"[INFO] 待追加号段 ({len(prefixes)}): {', '.join(prefixes)}")

    samples, before = {}, {}
    if existing_shards:
        # 只读打开不需要 LOCK；若有写进程（如另一个 generate/append）正在运行，
        # 后续写入打开时会因 LOCK 失败而报错，不会破坏数据。
        samples, before = sample_existing(db_root, args.shards)
        print(f"[INFO] 既有记录（估计）: {sum(before.values()):,}；"
              f"已抽样 {sum(len(v) for v in samples.values()):,} 条用于追加后校验")
        print("[WARN] 查询服务只读打开库，只能看到启动时的数据；"
              "请在追加完成后重启查询服务")

    stream_generate(prefixes, workers, args.shards, chunk_size, batch_size,
                    args.db_root, budget, builders)

    if existing_shards:
        checked = verify_existing(db_root, samples)
        _, after = sample_existing(db_root, args.shards, per_shard=0)
        print(f"[VERIFY] {checked:,} 条既有抽样记录全部完好；"
              f"记录数（估计）{sum(before.values()):,} -> {sum(after.values()):,}")
    print("APPEND COMPLETED")


if __name__ == "__main__":
    import multiprocessing as mp
    mp.freeze_support()
    main()
