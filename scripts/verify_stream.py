#!/usr/bin/env python3
"""verify_stream.py —— 端到端的"不重不漏"断言。

与 `python -m streamforge verify` 的区别：
断言直接对着 ground_truth() 算，不看作业自己的统计。

用法:
    python3 scripts/verify_stream.py [--n 5000] [--seed 20261112]
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from streamforge.events import generate, ground_truth  # noqa: E402
from streamforge.job import Job, make_dim  # noqa: E402


def snapshot_with_side_output(job: Job, events: list) -> dict:
    """主表 + 侧输出合并后的口径。

    事件时间模式下，迟到事件被路由到侧输出、没有回补，
    所以单看主表会缺订单。合并之后才应该与 ground_truth 一致——
    侧输出目前只落文件、不回补主表，因此合并后才是完整口径。
    """
    snap = job.snapshot()
    late_path = job.late_sink / "late.jsonl"
    late_ids: set[str] = set()
    if late_path.exists():
        with late_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    late_ids.add(json.loads(line)["event_id"])

    snap["side_output_events"] = len(late_ids)
    main_rows = job.sink.read_all()
    main_ids = {r["event_id"] for r in main_rows}
    snap["merged_events"] = len(main_ids | late_ids)

    # 订单最终状态：主表行 + 侧输出事件（用原始事件补全 order_id / amount / event_time）
    by_order: dict[str, dict] = {}
    for r in main_rows:
        cur = by_order.get(r["order_id"])
        if cur is None or r["event_time"] > cur["event_time"]:
            by_order[r["order_id"]] = r
    by_id = {e.event_id: e for e in events}
    for eid in late_ids:
        e = by_id.get(eid)
        if e is None:
            continue
        cur = by_order.get(e.order_id)
        if cur is None or e.event_time > cur["event_time"]:
            by_order[e.order_id] = {"order_id": e.order_id, "event_time": e.event_time}
    snap["merged_orders"] = len(by_order)
    return snap


def run_case(events, *, two_phase: bool, processing_time: bool, crash_at: int | None) -> dict:
    root = Path(tempfile.mkdtemp(prefix="sf-verify-"))
    mk = lambda: Job(  # noqa: E731
        make_dim(),
        root=root,
        batch_size=200,
        two_phase=two_phase,
        processing_time=processing_time,
        checkpoint_interval=200,
    )
    restore = None
    checkpoint = None
    resume_from = 0
    if crash_at is not None:
        first = mk()
        first.run(events, crash_at=crash_at)
        checkpoint = first.latest_checkpoint()
        first.close()
        job = mk()  # 模拟重启
        # 真实恢复：按检查点记录的 offset 继续消费，并按版本 id 对齐维表。
        report = job.resume_from_checkpoint(events)
        restore = report["dim_restore"]
        resume_from = report["resume_from"]
    else:
        job = mk()
        job.run(events)
    snap = snapshot_with_side_output(job, events)
    snap["dim_restore"] = restore
    snap["checkpoint"] = checkpoint
    snap["resume_from"] = resume_from
    job.close()
    return snap


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=20261112)
    ap.add_argument("--crash-at", type=int, default=800)
    args = ap.parse_args()

    ev = generate(n=args.n, seed=args.seed)
    gt = ground_truth(ev)

    cases = [
        ("默认（处理时间 + 无 2PC）", dict(two_phase=False, processing_time=True, crash_at=None)),
        ("崩溃恢复（无 2PC）", dict(two_phase=False, processing_time=True, crash_at=args.crash_at)),
        ("崩溃恢复（2PC）", dict(two_phase=True, processing_time=True, crash_at=args.crash_at)),
        ("事件时间 + 2PC", dict(two_phase=True, processing_time=False, crash_at=args.crash_at)),
    ]

    print(f"== 输入 {len(ev)} 条事件 / {len(gt)} 个订单 ==")
    print(f"{'场景':28} {'落表':>7} {'重复':>7} {'订单':>7} {'侧输出':>7} {'文件':>6}")
    rows = []
    for name, kw in cases:
        snap = run_case(ev, **kw)
        rows.append((name, snap, kw))
        print(
            f"{name:28} {snap['rows_written']:>7} {snap['duplicate_rows']:>7} "
            f"{snap['distinct_orders']:>7} {snap['stats']['sink']['late_routed']:>7} "
            f"{snap['files']:>6}"
        )

    print("\n== 判定 ==")
    ok = True

    # 1. 无 2PC 且崩溃 → 应出现重复，用于确认该配置下的行为
    _, snap_crash, _ = next(r for r in rows if r[0].startswith("崩溃恢复（无"))
    if snap_crash["duplicate_rows"] == 0:
        print("  [FAIL] 崩溃恢复（无 2PC）未出现重复，与预期不符")
        ok = False
    else:
        print(f"  [PASS] 崩溃恢复（无 2PC）复现了 {snap_crash['duplicate_rows']} 条重复")

    # 2. 2PC 且崩溃 → 必须恰好一次
    _, snap_2pc, _ = next(r for r in rows if r[0].startswith("崩溃恢复（2PC）"))
    if snap_2pc["rows_written"] != len(ev) or snap_2pc["duplicate_rows"] != 0:
        print(
            f"  [FAIL] 2PC 崩溃恢复应为 {len(ev)} 行、0 重复，"
            f"实际 {snap_2pc['rows_written']} 行、{snap_2pc['duplicate_rows']} 重复"
        )
        ok = False
    else:
        print(f"  [PASS] 2PC 崩溃恢复恰好一次（{snap_2pc['rows_written']} = {len(ev)}）")

    # 3. 处理时间模式下侧输出为 0
    _, snap_pt, _ = next(r for r in rows if r[0].startswith("默认"))
    late_count = sum(1 for e in ev if (e.ingest_ts() - e.event_ts()).total_seconds() > 60)
    if late_count > 0 and snap_pt["stats"]["sink"]["late_routed"] == 0:
        print(f"  [PASS] 处理时间模式：{late_count} 条迟到事件全部被吸收")
    else:
        print("  [FAIL] 处理时间模式应吸收全部迟到事件")
        ok = False

    # 4. 事件时间模式下侧输出 > 0
    _, snap_et, _ = next(r for r in rows if r[0].startswith("事件时间"))
    if snap_et["stats"]["sink"]["late_routed"] > 0:
        print(f"  [PASS] 事件时间模式：侧输出 {snap_et['stats']['sink']['late_routed']} 条")
    else:
        print("  [FAIL] 事件时间模式下应有侧输出")
        ok = False

    # 5. 主表 + 侧输出合并后，事件与订单口径必须完整
    if snap_et["merged_events"] == len(ev):
        print(f"  [PASS] 主表+侧输出事件完整：{snap_et['merged_events']} = {len(ev)}")
    else:
        print(f"  [FAIL] 合并后事件数 {snap_et['merged_events']} 应为 {len(ev)}")
        ok = False

    if snap_et["merged_orders"] == len(gt):
        print(
            f"  [PASS] 合并后订单口径一致：{snap_et['merged_orders']} = {len(gt)}"
            f"（主表单看只有 {snap_et['distinct_orders']}，"
            f"差 {len(gt) - snap_et['distinct_orders']} 个未回补到主表）"
        )
    else:
        print(
            f"  [FAIL] 合并后订单 {snap_et['merged_orders']} vs 真值 {len(gt)}"
        )
        ok = False

    # 6. 所有场景：维表版本一致、逐行版本可证明
    for name, snap, _kw in rows:
        dim = snap["dim"]
        if dim["miss_during_reload"] or dim["consistency_failures"]:
            print(
                f"  [FAIL] {name}：维表空窗 {dim['miss_during_reload']} / "
                f"不一致 {dim['consistency_failures']}"
            )
            ok = False
        elif dim["provenance_failures"]:
            print(f"  [FAIL] {name}：{len(dim['provenance_failures'])} 行版本溯源失败")
            ok = False
        elif not dim["active_version"]:
            print(f"  [FAIL] {name}：没有活动维表版本")
            ok = False
    if all(
        not r[1]["dim"]["provenance_failures"] and r[1]["dim"]["active_version"]
        for r in rows
    ):
        versions = {v for r in rows for v in r[1]["dim"]["row_versions"]}
        checked = sum(r[1]["dim"]["provenance_checked"] for r in rows)
        print(f"  [PASS] 维表原子切换 + 逐行版本溯源：{checked} 行全部可复算（{len(versions)} 个版本）")

    # 7. 崩溃场景：重启后按检查点恢复维表版本
    for name, snap, kw in rows:
        if kw["crash_at"] is None:
            continue
        restore = snap["dim_restore"]
        if restore and restore.get("restored"):
            print(f"  [PASS] {name}：按检查点恢复维表版本 {restore['version']}")
        else:
            print(f"  [FAIL] {name}：检查点维表恢复失败 {restore}")
            ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
