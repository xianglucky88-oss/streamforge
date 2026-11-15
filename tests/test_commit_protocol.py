"""端到端原子提交协议的状态机与故障注入测试。

覆盖需求点：
    * 暂存 / 预提交 / 检查点确认 / 正式发布 / 异常清理的明确状态机
    * 提交过程中进程终止（硬崩溃）
    * 对象存储延迟可见
    * 写入结局不确定（StoreIndeterminate）
    * 重复恢复的幂等性
    * 并发清理的安全性
    * 下游读取竞争（只见整批或不见）
    * 已提交 / 未完成 / 无法判定三分类，且无需人工删文件
"""

from __future__ import annotations

import tempfile
import threading
import zlib
from pathlib import Path

import pytest

from streamforge.commit import (
    CommitCoordinator,
    CrashInjected,
    LocalObjectStore,
    StoreIndeterminate,
    TxnState,
    TXN_PREFIX,
)


def _store(tmp_path: Path, **kw) -> LocalObjectStore:
    return LocalObjectStore(tmp_path, **kw)


def _coordinator(tmp_path: Path, **kw) -> CommitCoordinator:
    store = kw.pop("store", None) or _store(tmp_path)
    return CommitCoordinator(store, **kw)


# ---------------------------------------------------------------- 状态机


def test_txn_lifecycle_pending_prepared_committed(tmp_path):
    c = _coordinator(tmp_path)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"a":1}\n')
    assert c.visible_batch_ids() == []

    c.prepare(txn)
    assert c.visible_batch_ids() == []

    c.commit_pending()
    assert c.visible_batch_ids() == [0]
    assert c.read_committed() == [b'{"a":1}\n']
    assert c.state_view()[txn.txn_id] == TxnState.COMMITTED.value


def test_commit_is_idempotent_after_coordinator_rebuild(tmp_path):
    c = _coordinator(tmp_path)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"a":1}\n')
    c.prepare(txn)
    c.commit_pending()

    # 全新进程：只凭存储重建，不保留任何内存状态。
    c2 = _coordinator(tmp_path)
    assert c2.visible_batch_ids() == [0]
    c2.commit_pending()  # 重复提交必须是幂等 no-op
    assert len(c2.snapshot_entries()) == 1


def test_batch_id_monotonic_and_never_reused(tmp_path):
    c = _coordinator(tmp_path)
    assert c.begin_txn().batch_id == 0
    assert c.begin_txn().batch_id == 1
    with pytest.raises(ValueError):
        c.begin_txn(1)  # 已分配的批次号不能复用


# ---------------------------------------------------------------- 恢复三分类


def test_recover_aborts_data_only_txn(tmp_path):
    """崩溃在 prepare 之前：只有数据、无标记 → 确定未提交 → abort。"""
    c = _coordinator(tmp_path)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')

    report = _coordinator(tmp_path).recover(last_checkpoint_batch=0)
    assert report.aborted == [0]
    assert report.committed == []
    assert report.quarantined == []
    assert _coordinator(tmp_path).visible_batch_ids() == []


def test_recover_aborts_prepared_uncovered_txn(tmp_path):
    """PREPARED 但不被任何 checkpoint 覆盖 → 确定未提交 → 安全 abort。"""
    c = _coordinator(tmp_path)
    txn = c.begin_txn(3)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn)

    report = _coordinator(tmp_path).recover(last_checkpoint_batch=2)
    assert report.aborted == [3]


def test_recover_commits_journaled_txn_without_marker(tmp_path):
    """journal 有条目但 committed 标记缺失：journal 是最高事实 → 已提交。"""
    c = _coordinator(tmp_path)
    txn = c.begin_txn(1)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)
    c._commit_one(txn, checkpoint=100)
    (tmp_path / TXN_PREFIX / txn.txn_id / "committed.json").unlink()

    report = _coordinator(tmp_path).recover(last_checkpoint_batch=2)
    assert report.committed == [1]
    assert report.quarantined == []
    assert _coordinator(tmp_path).visible_batch_ids() == [1]


def test_recover_quarantines_undecidable_commit(tmp_path):
    """committed 标记存在但 journal 在 settle 窗口内始终缺失 → 无法判定 → 隔离。"""
    store = _store(tmp_path, visibility_lag_ticks=2)
    c = CommitCoordinator(store, visibility_attempts=3)
    txn = c.begin_txn(1)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)
    # 直接伪造一个 committed 标记，但 journal 永远没有对应 commit 条目。
    store.put(
        f"{TXN_PREFIX}{txn.txn_id}/committed.json",
        b'{"txn_id":"' + txn.txn_id.encode() + b'"}',
    )

    report = CommitCoordinator(_store(tmp_path, visibility_lag_ticks=2), visibility_attempts=3).recover(
        last_checkpoint_batch=2
    )
    assert report.quarantined == [1]
    assert report.committed == [] and report.aborted == []
    assert report.indeterminate_seen == 1

    c2 = _coordinator(tmp_path)
    assert any(v == TxnState.QUARANTINED.value for v in c2.state_view().values())
    # 隔离后批次号永久作废，下一个事务只能用 2。
    assert c2.begin_txn().batch_id == 2
    assert c2.visible_batch_ids() == []


def test_recover_settles_delayed_visibility_to_committed(tmp_path):
    """journal 实际已写，只是延迟可见：给足 settle 窗口后必须判已提交。"""
    store = _store(tmp_path, visibility_lag_ticks=1)
    c = CommitCoordinator(store, visibility_attempts=30)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)
    c._commit_one(txn, checkpoint=100)
    (tmp_path / TXN_PREFIX / txn.txn_id / "committed.json").unlink()

    report = CommitCoordinator(_store(tmp_path, visibility_lag_ticks=1), visibility_attempts=30).recover(
        last_checkpoint_batch=1
    )
    assert report.committed == [0]
    assert report.quarantined == []


# ---------------------------------------------------------------- 故障注入


def test_indeterminate_write_does_not_duplicate_data(tmp_path):
    store = _store(tmp_path, fail_on=("data.jsonl",), fail_durable=True)
    c = CommitCoordinator(store, visibility_attempts=10)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')

    # 物理上只能有一个 data.jsonl，不确定写入绝不能产生重复对象。
    physical = list((tmp_path / "_txn").rglob("data.jsonl"))
    assert len(physical) == 1
    assert txn.checksum == zlib.crc32(b'{"x":1}\n')
    assert c.stats.indeterminate_writes >= 1


def test_hard_crash_during_journal_append_is_quarantined(tmp_path):
    store = _store(tmp_path, crash_on=("entry-0000000001-txn-",))
    c = CommitCoordinator(store, visibility_attempts=10)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)

    with pytest.raises(CrashInjected):
        c._commit_one(txn, checkpoint=100)

    # 新进程在没有 checkpoint 信息时也不能贸然 abort（可能已对下游生效）。
    report = CommitCoordinator(_store(tmp_path), visibility_attempts=5).recover(
        last_checkpoint_batch=1
    )
    assert report.quarantined == [0]


def test_recovery_repairs_lost_current_pointer(tmp_path):
    store = _store(tmp_path)
    c = CommitCoordinator(store)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)
    c._commit_one(txn, checkpoint=100)
    store.delete("_journal/CURRENT")

    report = _coordinator(tmp_path).recover(last_checkpoint_batch=1)
    assert report.journal_repaired is True
    assert _coordinator(tmp_path).visible_batch_ids() == [0]


# ---------------------------------------------------------------- 清理与锁


def test_reconcile_never_deletes_undecided_txn(tmp_path):
    c = _coordinator(tmp_path)
    pending = c.begin_txn(0)
    c.write_data(pending, b'{"a":1}\n')
    other = c.begin_txn(1)
    c.write_data(other, b'{"b":1}\n')
    c.prepare(other)
    c.commit_pending()

    out = c.reconcile(grace_seconds=0)
    assert pending.txn_id not in out["deleted_txns"]
    # 未决事务目录必须原样保留，仍可在 state_view 里观测到。
    assert pending.txn_id in c.state_view()


def test_reconcile_respects_grace_window(tmp_path):
    c = _coordinator(tmp_path)
    for bid in range(3):
        txn = c.begin_txn(bid)
        c.write_data(txn, f"b{bid}\n".encode())
        c.prepare(txn)
    c.commit_pending()

    # 墙钟宽限未到：一个都不能删。
    assert c.reconcile()["deleted_txns"] == []
    # 宽限设为 0：三个已决事务目录全部可回收。
    assert len(c.reconcile(grace_seconds=0)["deleted_txns"]) == 3


def test_recovery_lock_excludes_concurrent_coordinator(tmp_path):
    c = _coordinator(tmp_path)
    with c._lock:
        with pytest.raises(RuntimeError):
            _coordinator(tmp_path)._lock.acquire()


def test_recovery_lock_lease_takeover_bumps_epoch(tmp_path):
    """持锁者租约过期后，新协调器可以接管，且 epoch 增加形成 fencing。"""
    from streamforge.commit import LOCK_KEY, _jsonb
    from datetime import datetime, timedelta, timezone

    store = _store(tmp_path)
    c1 = CommitCoordinator(store)
    c1._lock.acquire()
    old_epoch = c1._lock.epoch

    # 把锁文件改写成"租约已过期"的旧代际。
    expired = {
        "owner": c1._lock.owner,
        "epoch": old_epoch,
        "leased_until_wall": (
            datetime.now(timezone.utc) - timedelta(seconds=1)
        ).timestamp(),
    }
    store.put(LOCK_KEY, _jsonb(expired))

    c2 = CommitCoordinator(_store(tmp_path))
    c2._lock.acquire()
    assert c2._lock.epoch == old_epoch + 1

    # 旧持有者被 fence，后续提交 / 隔离动作必须被拒绝。
    with pytest.raises(RuntimeError):
        c1._lock.check_fence()


# ---------------------------------------------------------------- 水位与下游


def test_data_only_txn_keeps_batch_id_watermark(tmp_path):
    """只有数据、没有标记的 PENDING 目录，恢复后批次号也不能被复用。"""
    c = _coordinator(tmp_path)
    txn = c.begin_txn(2)
    c.write_data(txn, b'{"x":1}\n')

    c2 = _coordinator(tmp_path)
    c2.recover(last_checkpoint_batch=0)
    nxt = c2.begin_txn()
    assert nxt.batch_id == 3


def test_downstream_reader_never_sees_partial_publish(tmp_path):
    c = _coordinator(tmp_path)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n{"y":2}\n')
    c.prepare(txn)

    seen: list[tuple] = []

    def reader() -> None:
        # 发布期间持续轮询下游视角：只能观察到"没有"或"整批"。
        for _ in range(50):
            c.store.tick()
            seen.append(tuple(_coordinator(tmp_path).visible_batch_ids()))

    th = threading.Thread(target=reader)
    th.start()
    c.commit_pending()
    th.join()

    for snapshot in seen:
        assert snapshot in ((), (0,))


# ---------------------------------------------------------------- 端到端


def test_e2e_delayed_visibility_double_recovery_exactly_once(tmp_path):
    from streamforge.events import generate
    from streamforge.job import Job, make_dim

    ev = generate(n=1500, seed=7)

    def make_job() -> Job:
        return Job(
            make_dim(),
            root=tmp_path,
            two_phase=True,
            processing_time=True,
            batch_size=100,
            visibility_lag_ticks=1,
        )

    j1 = make_job()
    j1.run(ev, crash_at=550)
    # 崩溃时没有任何批次对下游可见。
    assert j1.snapshot()["rows_written"] == 0

    # 第一次恢复 + 完整重放。
    j2 = make_job()
    j2.run(ev)
    first = j2.snapshot()
    assert first["rows_written"] == len(ev)
    assert first["duplicate_rows"] == 0

    # 第二次恢复：重复对账必须幂等，结果与第一次完全一致。
    j3 = make_job()
    second = j3.snapshot()
    assert second["rows_written"] == len(ev)
    assert second["duplicate_rows"] == 0
    assert second["visible_batches"] == first["visible_batches"]


def test_e2e_checkpoint_covered_but_uncommitted_is_not_aborted(tmp_path):
    """checkpoint 已覆盖但 journal 无 commit 条目的批次绝不能 abort（否则重放双发）。"""
    from streamforge.job import _atomic_write_json
    from streamforge.commit import LocalObjectStore

    store = _store(tmp_path)
    c = CommitCoordinator(store)
    txn = c.begin_txn(0)
    c.write_data(txn, b'{"x":1}\n')
    c.prepare(txn, checkpoint=100)

    ck = tmp_path / "_checkpoints"
    ck.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(
        ck / "chk-00001.json",
        {"offset": 100, "next_batch_id": 1, "committed": True},
    )

    report = CommitCoordinator(LocalObjectStore(tmp_path)).recover(
        last_checkpoint_batch=1
    )
    assert report.quarantined == [0]
    assert report.aborted == []
    # 隔离批次对下游不可见，编号作废。
    assert CommitCoordinator(LocalObjectStore(tmp_path)).visible_batch_ids() == []
