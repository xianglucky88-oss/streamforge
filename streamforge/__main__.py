"""命令行入口。

    python -m streamforge replay           生成并回放一批事件
    python -m streamforge run              跑作业（可注入崩溃）
    python -m streamforge verify           一致性断言（含维表版本溯源）
    python -m streamforge dim-reload       演示维表原子版本切换

配置全部来自环境变量，便于在不同配置下回放同一份事件流。
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path

from .config import settings
from .events import generate, ground_truth, read_jsonl, write_jsonl
from .job import Job, make_dim


def cmd_replay(args) -> int:
    events = generate(n=args.n, seed=args.seed, late_ratio=args.late_ratio)
    path = Path(args.out)
    n = write_jsonl(events, path)
    gt = ground_truth(events)
    print(
        json.dumps(
            {
                "written": n,
                "path": str(path),
                "distinct_orders": len(gt),
                "late_events": sum(
                    1 for e in events if (e.ingest_ts() - e.event_ts()).total_seconds() > 60
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def cmd_run(args) -> int:
    path = Path(args.input)
    if path.exists():
        events = read_jsonl(path)
    else:
        events = generate(n=args.n, seed=args.seed)
        write_jsonl(events, path)
    job = Job(make_dim())
    report = job.run(events, crash_at=args.crash_at)
    snap = job.snapshot()
    job.close()
    print(json.dumps({"report": report, "snapshot": snap}, ensure_ascii=False, indent=2))
    return 0


def cmd_verify(args) -> int:
    path = Path(args.input)
    events = read_jsonl(path) if path.exists() else generate(n=args.n, seed=args.seed)
    gt = ground_truth(events)

    job = Job(make_dim(), checkpoint_interval=args.checkpoint_every)
    restore = None
    checkpoint = None
    if args.crash_at is not None:
        job.run(events, crash_at=args.crash_at)
        checkpoint = job.latest_checkpoint()
        job.close()
        job = Job(make_dim(), checkpoint_interval=args.checkpoint_every)  # 模拟重启
        # 按检查点恢复：offset 回退到最后一次检查点，维表版本按 id 对齐。
        report = job.resume_from_checkpoint(events)
        restore = report["dim_restore"]
    else:
        job.run(events)
    snap = job.snapshot()
    job.close()

    checks = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    add(
        "rows_no_dup",
        snap["rows_written"] == len(events),
        f"落表 {snap['rows_written']} 行 / 事件 {len(events)} 条（重复 {snap['duplicate_rows']}）",
    )
    add(
        "no_dup_events",
        snap["duplicate_rows"] == 0,
        f"重复事件 {snap['duplicate_rows']} 条",
    )
    add(
        "orders_complete",
        snap["distinct_orders"] == len(gt),
        f"落表订单 {snap['distinct_orders']} / 真值 {len(gt)}",
    )
    # 迟到数据：迟到事件必须要么被正确归窗、要么被侧输出，不能静默消失。
    # 判定口径：处理时间模式下，迟到事件会被当作正常事件落表（看似没丢），
    # 但它被归到了错误的窗口，业务侧按 event_time 汇总时会发现缺数。
    late_routed = snap["stats"]["sink"]["late_routed"]
    expected_late = sum(1 for e in events if (e.ingest_ts() - e.event_ts()).total_seconds() > 60)
    if not settings.processing_time:
        late_ok = late_routed > 0 or expected_late == 0
        late_detail = (
            f"事件时间模式：迟到事件 {expected_late} 条，侧输出 {late_routed} 条"
        )
    else:
        # 处理时间模式下侧输出始终为 0。
        late_ok = expected_late == 0
        late_detail = (
            f"处理时间模式：迟到事件 {expected_late} 条，侧输出 {late_routed} 条"

        )
    add("late_events_accounted", late_ok, late_detail)
    # 小文件
    add(
        "file_count",
        snap["files"] <= args.max_files,
        f"文件数 {snap['files']}（阈值 {args.max_files}）",
    )
    # 维表：必须有活动版本，且 reload 空窗为 0。
    dim = snap["dim"]
    add(
        "dim_loaded",
        dim["reloads"] >= 1,
        f"维表版本切换 {dim['reloads']} 次，活动版本 {dim['active_version']}",
    )
    add(
        "dim_snapshot_consistent",
        dim["miss_during_reload"] == 0 and dim["consistency_failures"] == 0,
        f"reload 空窗 {dim['miss_during_reload']}，版本不一致 {dim['consistency_failures']}",
    )
    add(
        "dim_version_provenance",
        dim["provenance_checked"] > 0 and not dim["provenance_failures"],
        f"逐行校验 {dim['provenance_checked']} 行，失败 {len(dim['provenance_failures'])}，"
        f"涉及版本 {len(dim['row_versions'])} 个",
    )
    if checkpoint is not None and checkpoint.get("dim_version"):
        add(
            "dim_restored_from_checkpoint",
            bool(restore and restore["restored"]),
            f"检查点要求版本 {checkpoint['dim_version']}，恢复结果 {restore}",
        )

    if args.json:
        print(
            json.dumps(
                {"snapshot": snap, "checks": checks, "dim_restore": restore},
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(f"== 事件 {len(events)} 条，订单 {len(gt)} 个 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")
    return 0 if all(c["ok"] for c in checks) else 1


def cmd_dim_reload(args) -> int:
    """演示维表版本切换：加载期间在线查询始终看到完整版本。

    加载器按“分片”产出，每两片之间停一下，模拟慢速全量拉取。事件线程在
    加载进行中持续查询；由于构建与发布分离，查询只会落在完整旧版本或
    完整新版本上，不会出现空维度或新旧混用。
    """
    base = make_dim()
    # 新快照内容与旧版本不同（模拟上游改了店铺名/区域），这样演示能看到切换。
    fresh = [{**r, "shop_name": r["shop_name"] + "*", "tier": "S"} for r in base]
    shards = [fresh[i:: args.shards] for i in range(args.shards)]
    loaded_shards = {"n": 0}

    def slow_loader():
        for shard in shards:
            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)
            loaded_shards["n"] += 1
            yield from shard

    root = Path(tempfile.mkdtemp(prefix="sf-dim-"))
    job = Job(base, root=root, two_phase=False, processing_time=True)
    before = job.dim.report()
    events = generate(n=args.n, seed=args.seed)

    # 后台开始构建新版本。
    job.dim.request_refresh(slow_loader)

    # 构建期间处理事件：记录每条事件看到的版本 id。
    versions_seen: dict[str, int] = {}
    mixed = 0
    known = {r["shop_id"] for r in base}
    for i, ev in enumerate(events):
        # 放慢消费速度，让整段加载确实横跨事件处理。
        if args.consume_ms:
            time.sleep(args.consume_ms / 1000)
        version = job.dim.current
        dim = version.lookup(ev.shop_id) if version else None
        vid = version.id if version else "none"
        versions_seen[vid] = versions_seen.get(vid, 0) + 1
        if dim is None and ev.shop_id in known:
            mixed += 1  # 该 shop 本应存在却落空 → 说明读到了不完整版本
        job._consume(i, ev)

    job.dim.wait_idle(timeout=30)
    job._flush()
    after = job.dim.report()
    job.close()

    print(
        json.dumps(
            {
                "before": {
                    "active_version": before["active_version"],
                    "rows": before["active_rows"],
                },
                "after": {
                    "active_version": after["active_version"],
                    "rows": after["active_rows"],
                    "loads": after["loads"],
                    "failures": after["load_failures"],
                },
                "shards_loaded": loaded_shards["n"],
                "lookups": after["lookups"],
                "lookups_missed": after["lookups_missed"],
                "miss_during_reload": after["miss_during_reload"],
                "events_missing_known_shop": mixed,
                "versions_seen": versions_seen,
                "events_processed": len(events),
                "note": "构建期间的查询全部落在完整版本上；切换是一次引用赋值",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="streamforge")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("replay", help="生成事件流")
    p.add_argument("--n", type=int, default=5000)
    p.add_argument("--seed", type=int, default=20261112)
    p.add_argument("--late-ratio", type=float, default=0.04)
    p.add_argument("--out", default="data/events.jsonl")
    p.set_defaults(func=cmd_replay)

    r = sub.add_parser("run", help="跑作业")
    r.add_argument("--input", default="data/events.jsonl")
    r.add_argument("--n", type=int, default=5000)
    r.add_argument("--seed", type=int, default=20261112)
    r.add_argument("--crash-at", type=int, default=None)
    r.set_defaults(func=cmd_run)

    v = sub.add_parser("verify", help="一致性断言")
    v.add_argument("--input", default="data/events.jsonl")
    v.add_argument("--n", type=int, default=5000)
    v.add_argument("--seed", type=int, default=20261112)
    v.add_argument("--crash-at", type=int, default=None)
    v.add_argument("--checkpoint-every", type=int, default=200)
    v.add_argument("--max-files", type=int, default=200)
    v.add_argument("--json", action="store_true")
    v.set_defaults(func=cmd_verify)

    d = sub.add_parser("dim-reload", help="演示维表版本切换")
    d.add_argument("--n", type=int, default=2000)
    d.add_argument("--seed", type=int, default=20261112)
    d.add_argument("--shards", type=int, default=8, help="加载器分片数")
    d.add_argument("--delay-ms", type=int, default=5, help="每片之间的延迟")
    d.add_argument("--consume-ms", type=int, default=1, help="每条事件的处理耗时")
    d.set_defaults(func=cmd_dim_reload)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
