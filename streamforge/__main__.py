"""命令行入口。

    python -m streamforge replay           生成并回放一批事件
    python -m streamforge run              跑作业（可注入崩溃）
    python -m streamforge verify           一致性断言
    python -m streamforge dim-reload       模拟维表 reload

配置全部来自环境变量，便于在不同配置下回放同一份事件流。
"""

from __future__ import annotations

import argparse
import json
import sys
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
    print(json.dumps({"report": report, "snapshot": snap}, ensure_ascii=False, indent=2))
    return 0


def cmd_verify(args) -> int:
    path = Path(args.input)
    events = read_jsonl(path) if path.exists() else generate(n=args.n, seed=args.seed)
    gt = ground_truth(events)

    job = Job(make_dim())
    if args.crash_at is not None:
        job.run(events, crash_at=args.crash_at)
        job = Job(make_dim())  # 模拟重启
        job.run(events)
    else:
        job.run(events)
    snap = job.snapshot()

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
            f"（按 ingest_time 归窗，迟到被静默吸收）"
        )
    add("late_events_accounted", late_ok, late_detail)
    # 小文件
    add(
        "file_count",
        snap["files"] <= args.max_files,
        f"文件数 {snap['files']}（阈值 {args.max_files}）",
    )
    # 维表
    add(
        "dim_loaded",
        snap["dim"]["reloads"] >= 1,
        f"维表 reload {snap['dim']['reloads']} 次",
    )

    if args.json:
        print(json.dumps({"snapshot": snap, "checks": checks}, ensure_ascii=False, indent=2))
    else:
        print(f"== 事件 {len(events)} 条，订单 {len(gt)} 个 ==")
        for c in checks:
            print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['name']}: {c['detail']}")
        failed = sum(1 for c in checks if not c["ok"])
        print(f"\n结果: {len(checks) - failed}/{len(checks)} 通过")
    return 0 if all(c["ok"] for c in checks) else 1


def cmd_dim_reload(args) -> int:
    """演示维表全量 reload 期间的空窗。

    reload 的实现是「先 clear 再逐条填」。如果在 clear 之后、填完之前
    有查询进来，就会落空。这里用一个可中断的填充过程来复现：
    清空后先查一次（模拟此时到来的事件），再填完。
    """
    dim = make_dim()
    job = Job(dim, two_phase=False, processing_time=True)
    events = generate(n=args.n, seed=args.seed)
    job.run(events[: len(events) // 2])
    before = job.dim.miss_during_reload

    # 手工复现 reload 的空窗：clear -> 查询 -> 填充
    job.dim.rows.clear()
    for r in events[len(events) // 2 : len(events) // 2 + 5]:
        # 空窗期内到来的事件，查维表必然落空
        if job.dim.lookup(r.shop_id) is None:
            job.stats.dim_misses += 1
            job.dim.miss_during_reload += 1
    for row in dim:
        job.dim.rows[row["shop_id"]] = row
    job.dim.reloads += 1

    job.run(events[len(events) // 2 :])
    print(
        json.dumps(
            {
                "miss_before_reload": before,
                "miss_during_reload": job.dim.miss_during_reload,
                "reloads": job.dim.reloads,
                "note": "clear 之后、填充之前到来的 5 条事件全部落空",
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
    v.add_argument("--max-files", type=int, default=200)
    v.add_argument("--json", action="store_true")
    v.set_defaults(func=cmd_verify)

    d = sub.add_parser("dim-reload", help="演示维表 reload 空窗")
    d.add_argument("--n", type=int, default=2000)
    d.add_argument("--seed", type=int, default=20261112)
    d.set_defaults(func=cmd_dim_reload)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
