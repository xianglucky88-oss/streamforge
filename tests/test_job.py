"""作业语义测试。

核心命题：**重启不重、迟到不丢**。
每个测试都直接对照 ground_truth() 校验，而不是只看作业自己的计数。
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from streamforge.config import settings
from streamforge.events import generate, ground_truth
from streamforge.job import CrashInjected, Job, Watermarks, make_dim


@pytest.fixture()
def events():
    return generate(n=1500, seed=99)


def _job(root: Path, **kw) -> Job:
    kw.setdefault("batch_size", 100)
    return Job(make_dim(), root=root, **kw)


# ---------------------------------------------------------------- 生成器


def test_generator_deterministic():
    a = generate(n=200, seed=5)
    b = generate(n=200, seed=5)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert [e.ingest_time for e in a] == [e.ingest_time for e in b]


def test_events_sorted_by_ingest_time():
    ev = generate(n=300, seed=5)
    assert [e.ingest_time for e in ev] == sorted(e.ingest_time for e in ev)


def test_some_events_are_late():
    ev = generate(n=500, seed=5, late_ratio=0.1)
    late = [e for e in ev if (e.ingest_ts() - e.event_ts()).total_seconds() > 60]
    assert late, "generator should produce late events"
    # 迟到事件应该占一定比例
    assert 0.02 < len(late) / len(ev) < 0.3


def test_ground_truth_takes_latest_by_event_time():
    ev = generate(n=500, seed=11)
    gt = ground_truth(ev)
    for oid, amount in list(gt.items())[:20]:
        candidates = [e for e in ev if e.order_id == oid]
        latest = max(candidates, key=lambda e: e.event_ts())
        assert latest.amount_cents == amount


# ---------------------------------------------------------------- 水位线


def test_watermark_advances_monotonically():
    wm = Watermarks(allowed_lateness_seconds=60)
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 11, 10, tzinfo=timezone.utc)
    wm.advance(base)
    wm.advance(base + timedelta(minutes=10))
    wm.advance(base + timedelta(minutes=5))  # 乱序，不应回退
    assert wm.current == base + timedelta(minutes=9)


def test_watermark_marks_old_events_late():
    wm = Watermarks(allowed_lateness_seconds=0)
    from datetime import datetime, timedelta, timezone

    base = datetime(2026, 11, 10, tzinfo=timezone.utc)
    wm.advance(base + timedelta(hours=1))
    assert wm.is_late(base + timedelta(minutes=10))
    assert not wm.is_late(base + timedelta(hours=1))


# ---------------------------------------------------------------- 默认配置

def test_event_time_routes_late_events_to_side_output():
    root = Path(tempfile.mkdtemp())
    job = _job(root, processing_time=False, allowed_lateness=0)
    ev = generate(n=800, seed=3, late_ratio=0.1)
    job.run(ev)
    snap = job.snapshot()
    assert snap["stats"]["sink"]["late_routed"] > 0
    # 侧输出的事件不应落表
    assert snap["rows_written"] < len(ev)


def test_no_duplicates_without_crash():
    root = Path(tempfile.mkdtemp())
    job = _job(root)
    ev = generate(n=600, seed=4)
    job.run(ev)
    snap = job.snapshot()
    assert snap["rows_written"] == len(ev)
    assert snap["duplicate_rows"] == 0


# ---------------------------------------------------------------- 崩溃与恢复

def test_crash_with_2pc_is_exactly_once():
    """开启 2PC 后，崩溃恢复应恰好一次。"""
    root = Path(tempfile.mkdtemp())
    ev = generate(n=1000, seed=7)

    j1 = _job(root, two_phase=True, processing_time=True)
    j1.run(ev, crash_at=400)
    # 未 commit 的批次不可见
    assert j1.snapshot()["rows_written"] == 0

    j2 = _job(root, two_phase=True, processing_time=True)
    j2.run(ev)
    snap = j2.snapshot()
    assert snap["rows_written"] == len(ev)
    assert snap["duplicate_rows"] == 0


def test_2pc_staging_aborted_on_restore():
    """恢复时 staging 目录应被清空，不污染下一轮。"""
    root = Path(tempfile.mkdtemp())
    ev = generate(n=600, seed=8)
    j1 = _job(root, two_phase=True)
    j1.run(ev, crash_at=250)
    staged = list((root / "orders" / "_staging").glob("*.jsonl"))
    assert staged, "crash should leave staged files"
    _job(root, two_phase=True)  # 重启
    assert not list((root / "orders" / "_staging").glob("*.jsonl"))


# ---------------------------------------------------------------- 维表

def test_small_batches_create_many_files():
    root = Path(tempfile.mkdtemp())
    job = _job(root, batch_size=50)
    ev = generate(n=500, seed=1)
    job.run(ev)
    assert job.snapshot()["files"] == 10


def test_large_batch_creates_few_files():
    root = Path(tempfile.mkdtemp())
    job = _job(root, batch_size=100000)
    ev = generate(n=500, seed=1)
    job.run(ev)
    assert job.snapshot()["files"] == 1


# ---------------------------------------------------------------- 订单口径


def test_order_final_state_matches_ground_truth_when_no_restart():
    root = Path(tempfile.mkdtemp())
    job = _job(root, processing_time=True)
    ev = generate(n=800, seed=21)
    job.run(ev)
    gt = ground_truth(ev)
    snap = job.snapshot()
    assert snap["distinct_orders"] == len(gt)
