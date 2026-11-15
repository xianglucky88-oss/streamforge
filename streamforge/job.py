"""流式作业运行时：算子语义与 Flink 对齐。

关键概念在本文件里的对应关系：

    Flink 概念                 本实现
    -----------------------    ------------------------------------------
    Source                     iter(events)
    Watermark                   Watermarks 类，单调递增的下界
    KeyedState (ValueState)    DimState，TTL 到期即清除
    SideOutput (late data)     sink.late_events
    Checkpoint                 周期性把 offset 写进 _checkpoints/
    TwoPhaseCommitSink         2PC 开启时走 CommitCoordinator 原子提交
    Processing time            用 ingest_time 做窗口归属
    Event time                 用 event_time 做窗口归属

几个关键行为都在 `Job._consume` 与 `Job._checkpoint` 里：
    * 默认用 processing time（#2）→ 迟到数据被静默丢弃
    * 2PC 默认关闭（#1）→ 作业重启后重复写入
    * 维表 reload 采用全量覆盖
    * 每批次一个文件（#4）→ 小文件爆炸

2PC 路径的完整协议（暂存 → 预提交 → 检查点确认 → 正式发布 → 异常清理）
见 :mod:`streamforge.commit`。
"""

from __future__ import annotations

import json
import os
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .commit import (
    CommitCoordinator,
    CrashInjected,
    LocalObjectStore,
    TxnState,
)
from .config import settings
from .events import OrderEvent

__all__ = [
    "CrashInjected",
    "Job",
    "Sink",
    "Watermarks",
    "DimState",
    "make_dim",
]


# ---------------------------------------------------------------- 水位线


@dataclass
class Watermarks:
    """事件时间水位线。

    Flink 的语义是：水位线 W 表示"不会再看到 event_time < W 的事件"。
    水位线 = max_seen_event_time - allowed_lateness。
    """

    allowed_lateness_seconds: int = 0
    _max_seen: datetime | None = None

    def advance(self, ts: datetime) -> None:
        if self._max_seen is None or ts > self._max_seen:
            self._max_seen = ts

    @property
    def current(self) -> datetime | None:
        if self._max_seen is None:
            return None
        return self._max_seen - timedelta(seconds=self.allowed_lateness_seconds)

    def is_late(self, ts: datetime) -> bool:
        cur = self.current
        return cur is not None and ts < cur


# ---------------------------------------------------------------- 维表


@dataclass
class DimState:
    """维表状态。

    真实 Flink 里这是一个被广播的 MapState。这里的实现复刻了"全量 reload"的行为：
    reload 时先清空、再逐条写入。
    """

    rows: dict[str, dict] = field(default_factory=dict)
    reloads: int = 0
    last_reload_at: datetime | None = None
    # reload 过程中查询落空的次数。
    miss_during_reload: int = 0

    def reload(self, supplier_rows: Iterable[dict]) -> None:
        # 先清空再逐条写入。
        self.rows.clear()
        for r in supplier_rows:
            self.rows[r["shop_id"]] = r
        self.reloads += 1
        self.last_reload_at = datetime.now(timezone.utc)

    def lookup(self, shop_id: str) -> dict | None:
        return self.rows.get(shop_id)


# ---------------------------------------------------------------- Sink


@dataclass
class SinkStats:
    written: int = 0
    files: int = 0
    late_dropped: int = 0
    late_routed: int = 0
    duplicates: int = 0
    commits: int = 0
    staging_writes: int = 0
    reconciled: int = 0
    quarantined: int = 0
    indeterminate_writes: int = 0
    visibility_retries: int = 0

    def to_dict(self) -> dict:
        return {
            "written": self.written,
            "files": self.files,
            "late_dropped": self.late_dropped,
            "late_routed": self.late_routed,
            "duplicates": self.duplicates,
            "commits": self.commits,
            "staging_writes": self.staging_writes,
            "reconciled": self.reconciled,
            "quarantined": self.quarantined,
            "indeterminate_writes": self.indeterminate_writes,
            "visibility_retries": self.visibility_retries,
        }


def _atomic_write_json(path: Path, payload: dict) -> None:
    """先写同目录临时文件再原子替换：崩溃不会留下半个 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp-" + os.urandom(4).hex())
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(tmp, path)


class Sink:
    """Iceberg sink 的文件级模拟，支持两条写入路径。

    * ``two_phase=False``：直接写正式目录（旧行为，崩溃重放会重复）。
    * ``two_phase=True``：数据先以事务形式暂存到对象存储，只有被
      :class:`~streamforge.commit.CommitCoordinator` 追加进 journal 的批次
      才对下游可见；恢复时按标记 / journal / checkpoint 三源对账。
    """

    def __init__(
        self,
        root: Path,
        two_phase: bool,
        *,
        visibility_lag_ticks: int = 0,
        visibility_attempts: int | None = None,
        lease_seconds: float | None = None,
    ) -> None:
        self.root = root
        self.two_phase = two_phase
        self.stats = SinkStats()
        self.root.mkdir(parents=True, exist_ok=True)

        # 活动事务句柄：batch_id -> TxnHandle。
        self._active: dict[int, TxnHandle] = {}

        if not two_phase:
            # ---- 旧路径：直接写正式目录 ----
            (root / "_staging").mkdir(exist_ok=True)
            existing = [int(p.stem.split("-")[1]) for p in self.root.glob("batch-*.jsonl")]
            self._committed_batch = (max(existing) + 1) if existing else 0
            self.stats.written = sum(_count_lines(p) for p in self.root.glob("batch-*.jsonl"))
            self.stats.files = len(existing)
            self.stats.commits = len(existing)
            for staged in (self.root / "_staging").glob("batch-*.jsonl"):
                staged.unlink(missing_ok=True)
            self.coordinator = None
            self.store = None
            return

        # ---- 2PC 路径：对象存储 + 提交协调器 ----
        self.store = LocalObjectStore(
            self.root, visibility_lag_ticks=visibility_lag_ticks
        )
        self.coordinator = CommitCoordinator(
            self.store,
            visibility_attempts=(
                settings.visibility_attempts
                if visibility_attempts is None
                else visibility_attempts
            ),
        )
        self._recover_on_start()

    # ---------- 2PC：启动恢复 ----------

    def _latest_checkpoint_batch(self) -> int | None:
        """读最近一次成功 checkpoint 的 next_batch_id（没有则 None）。"""
        ck = self.root.parent / "_checkpoints"
        files = sorted(ck.glob("chk-*.json")) if ck.exists() else []
        if not files:
            return None
        for path in reversed(files):
            try:
                meta = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if meta.get("committed"):
                return int(meta.get("next_batch_id", meta.get("batch_id", 0)))
        return None

    def _recover_on_start(self) -> None:
        """三源对账：已提交 / 未完成 abort / 无法判定隔离，全程无需人工删文件。"""
        assert self.coordinator is not None
        last_cp = self._latest_checkpoint_batch()
        report = self.coordinator.recover(last_checkpoint_batch=last_cp)
        # 有了 abort 结论的事务目录可以立刻物理清理；隔离区永久保留。
        cleanup = self.cleanup_aborted()
        self.stats.reconciled = cleanup
        self.stats.quarantined = len(report.quarantined)
        self.stats.commits = len(self.coordinator.snapshot_entries())
        self.stats.files = self.stats.commits
        self.stats.written = sum(
            block.count(b"\n") for block in self.coordinator.read_committed()
        )
        self.stats.indeterminate_writes = self.coordinator.stats.indeterminate_writes
        self.stats.visibility_retries = self.coordinator.stats.visibility_retries
        self._recovery_report = report

    def cleanup_aborted(self) -> int:
        """物理删除已有 abort 结论的事务目录，返回删除的对象数。

        只动 journal 里已有结论的目录；隔离区与未决事务永不删除，
        因此并发清理不可能删掉正在提交的事务。
        """
        if self.coordinator is None or self.store is None:
            return 0
        n = 0
        for txn_id in list(self.coordinator.aborted_txn_ids()):
            for key in self.store.list(f"_txn/{txn_id}/"):
                self.store.delete(key)
                n += 1
            self.coordinator.forget_txn(txn_id)
        return n

    # ---------- 写入 ----------

    def write_batch(self, batch_id: int, rows: list[dict]) -> None:
        name = f"batch-{batch_id:06d}.jsonl"
        if not self.two_phase:
            # 未启用 2PC 时直接写正式目录。
            # 作业在这之后崩溃 → 重启从上一个 checkpoint 重放 → 重复写入。
            final = self.root / name
            _write_rows(final, rows)
            self.stats.written += len(rows)
            self.stats.files += 1
            self.stats.commits += 1
            return

        assert self.coordinator is not None
        txn = self.coordinator.begin_txn(batch_id)
        raw = _encode_rows(rows)
        self.coordinator.write_data(txn, raw)
        self._active[batch_id] = txn
        self.stats.staging_writes += len(rows)

    def prepare(self, upto_batch: int, checkpoint: int | None = None) -> None:
        """预提交：把 [.., upto_batch] 的暂存事务翻成 PREPARED。"""
        if self.coordinator is None:
            return
        for bid in sorted(b for b in self._active if b <= upto_batch):
            self.coordinator.prepare(self._active[bid], checkpoint=checkpoint)

    def commit(self, upto_batch: int) -> int:
        """正式发布所有已 PREPARED 的事务。返回新发布的批次数。

        旧路径（无 2PC）保持原语义：从 staging 拷贝到正式目录。
        """
        if not self.two_phase:
            return 0
        assert self.coordinator is not None
        before = len(self.coordinator.snapshot_entries())
        entries = self.coordinator.commit_pending()
        published = len(entries)
        # 释放已完成事务的内存句柄。
        for bid in [b for b, h in self._active.items() if h.state == TxnState.COMMITTED]:
            self._active.pop(bid, None)
        after = len(self.coordinator.snapshot_entries())
        self.stats.commits = after
        self.stats.files = after
        self.stats.written = sum(
            block.count(b"\n") for block in self.coordinator.read_committed()
        )
        self.stats.quarantined = self.coordinator.stats.quarantined
        self.stats.indeterminate_writes = self.coordinator.stats.indeterminate_writes
        self.stats.visibility_retries = self.coordinator.stats.visibility_retries
        return published if published else after - before

    # ---------- 读取 ----------

    def read_all(self) -> list[dict]:
        if not self.two_phase:
            rows: list[dict] = []
            for p in sorted(self.root.glob("batch-*.jsonl")):
                rows.extend(_read_rows(p))
            return rows

        assert self.coordinator is not None
        out: list[dict] = []
        for block in self.coordinator.read_committed():
            for line in block.decode("utf-8").splitlines():
                if line.strip():
                    out.append(json.loads(line))
        return out

    def visible_batch_ids(self) -> list[int]:
        """下游视角已发布的批次号（只来自 journal，绝不来自目录扫描）。"""
        if self.coordinator is None:
            return [int(p.stem.split("-")[1]) for p in self.root.glob("batch-*.jsonl")]
        return self.coordinator.visible_batch_ids()

    def file_count(self) -> int:
        if self.coordinator is None:
            return len(list(self.root.glob("batch-*.jsonl")))
        return len(self.coordinator.snapshot_entries())

    def next_batch_id(self) -> int:
        """批次号高水位 + 1；隔离 / abort 的编号永久作废，绝不复用。"""
        if self.coordinator is None:
            return self._committed_batch
        return self.coordinator._max_known_batch_id() + 1


def _encode_rows(rows: list[dict]) -> bytes:
    return ("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)).encode("utf-8")


def _write_rows(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")


def _read_rows(path: Path) -> list[dict]:
    out = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.append(json.loads(line))
    return out


def _count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as fh:
        return sum(1 for line in fh if line.strip())


# ---------------------------------------------------------------- 作业


@dataclass
class JobStats:
    consumed: int = 0
    emitted: int = 0
    late: int = 0
    checkpoints: int = 0
    restarts: int = 0
    dim_misses: int = 0

    def to_dict(self) -> dict:
        return {
            "consumed": self.consumed,
            "emitted": self.emitted,
            "late": self.late,
            "checkpoints": self.checkpoints,
            "restarts": self.restarts,
            "dim_misses": self.dim_misses,
        }


class Job:
    """入湖作业。

    调用方式：

        job = Job(dim_rows)
        report = job.run(events)          # 完整跑一遍
        report = job.run(events, crash_at=1200)   # 在第 1200 条时"崩溃"
    """

    def __init__(
        self,
        dim_rows: list[dict],
        root: Path | None = None,
        two_phase: bool | None = None,
        processing_time: bool | None = None,
        allowed_lateness: int | None = None,
        batch_size: int | None = None,
        visibility_lag_ticks: int | None = None,
    ) -> None:
        self.root = root or settings.sink_root
        settings.ensure()
        self.two_phase = settings.two_phase_commit if two_phase is None else two_phase
        self.processing_time = settings.processing_time if processing_time is None else processing_time
        self.allowed_lateness = (
            settings.allowed_lateness_seconds if allowed_lateness is None else allowed_lateness
        )
        self.batch_size = settings.batch_size if batch_size is None else batch_size

        self.sink = Sink(
            self.root / "orders",
            two_phase=self.two_phase,
            visibility_lag_ticks=(
                settings.visibility_lag_ticks
                if visibility_lag_ticks is None
                else visibility_lag_ticks
            ),
        )
        self.late_sink = self.root / "late_events"
        self.late_sink.mkdir(parents=True, exist_ok=True)
        self.dim = DimState()
        self.dim.reload(dim_rows)
        self.wm = Watermarks(allowed_lateness_seconds=self.allowed_lateness)
        self.stats = JobStats()
        # 批次号从存储高水位之后继续：隔离 / abort 的编号不复用。
        self._batch_id = self.sink.next_batch_id()
        self._batch: list[dict] = []
        # 最近一次持久化 checkpoint 的 source offset，重启后从这里继续。
        self._resume_offset = self._latest_checkpoint_offset()

    def _latest_checkpoint_offset(self) -> int:
        ck = self.root / "_checkpoints"
        files = sorted(ck.glob("chk-*.json")) if ck.exists() else []
        for path in reversed(files):
            try:
                meta = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            return int(meta.get("offset", 0))
        return 0

    # ---------- 主循环 ----------

    def run(self, events: list[OrderEvent], crash_at: int | None = None) -> dict:
        """消费事件。

        crash_at: 处理到第 N 条时模拟进程被杀（不 commit、不 checkpoint）。

        重启时自动从最近一次持久化 checkpoint 的 source offset 继续：
        该偏移之前的事件要么已发布（journal 可查）、要么已隔离（数据保留在
        隔离区），重放它们只会造成双发，因此必须跳过。
        """
        reported: dict = {}
        try:
            for i, ev in enumerate(events):
                if i < self._resume_offset:
                    continue
                if crash_at is not None and i == crash_at:
                    raise CrashInjected(f"injected crash at event {i}")
                self._consume(i, ev)
        except CrashInjected as exc:
            self.stats.restarts += 1
            reported["crash"] = str(exc)
        self._flush()
        # 正常结束时补一次最终提交。真实 Flink 在作业停止时也会做
        # 最后一次 checkpoint + commit；崩溃路径不会走到这里。
        if "crash" not in reported and self.two_phase:
            self._publish(checkpoint=None)
        return {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict(), **reported}

    def resume(self, events: list[OrderEvent], from_index: int) -> dict:
        """从上次 checkpoint 的位置继续消费。

        无 2PC 时 [from_index, crash_at) 已写入的行会被再次写入；
        2PC 时未发布的事务在启动恢复中已被 abort / 隔离，重放恰好一次。
        """
        return self.run(events[from_index:])

    def _consume(self, idx: int, ev: OrderEvent) -> None:
        self.stats.consumed += 1
        ts = ev.ingest_ts() if self.processing_time else ev.event_ts()

        # 水位线只在事件时间模式下推进
        if not self.processing_time:
            self.wm.advance(ev.event_ts())
            if self.wm.is_late(ev.event_ts()):
                # 只有开启事件时间才会走侧输出，
                # 而默认配置是 processing time，所以这条分支默认不生效。
                self.stats.late += 1
                self.sink.stats.late_routed += 1
                self._route_late(ev, "event_time behind watermark")
                return

        dim = self.dim.lookup(ev.shop_id)
        if dim is None:
            self.stats.dim_misses += 1
            self.dim.miss_during_reload += 1
            dim = {"shop_id": ev.shop_id, "shop_name": None, "region": None}

        row = {
            "event_id": ev.event_id,
            "order_id": ev.order_id,
            "shop_id": ev.shop_id,
            "shop_name": dim.get("shop_name"),
            "region": dim.get("region"),
            "status": ev.status,
            "amount_cents": ev.amount_cents,
            "channel": ev.channel,
            "event_time": ev.event_time,
            "ingest_time": ev.ingest_time,
            # 窗口归属使用上面选定的时间字段
            "window_date": _window_date(ts),
        }
        self._batch.append(row)
        self.stats.emitted += 1

        if len(self._batch) >= self.batch_size:
            self._flush()

        if (idx + 1) % settings.checkpoint_interval == 0:
            self._checkpoint(idx + 1)

    def _route_late(self, ev: OrderEvent, reason: str) -> None:
        path = self.late_sink / "late.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "event_id": ev.event_id,
                        "event_time": ev.event_time,
                        "reason": reason,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    def _flush(self) -> None:
        if not self._batch:
            return
        self.sink.write_batch(self._batch_id, self._batch)
        self._batch = []
        self._batch_id += 1

    def _publish(self, *, checkpoint: int | None) -> int:
        """预提交 → 正式发布。下游只能通过 journal 看到整批结果。"""
        if not self.two_phase:
            return self.sink.commit(self._batch_id - 1)
        self.sink.prepare(self._batch_id - 1, checkpoint=checkpoint)
        return self.sink.commit(self._batch_id - 1)

    def _checkpoint(self, offset: int) -> None:
        self.stats.checkpoints += 1
        # 先把内存里的批次落进事务目录，保证 checkpoint 覆盖的批次都有数据。
        self._flush()

        if self.two_phase:
            # 严格的 2PC 顺序：
            #   1. prepare：PREPARED 标记持久化（事务进入"必须给结论"窗口）
            #   2. checkpoint 事实落盘（source offset + next_batch_id）
            #   3. commit：journal 追加发布
            # 崩溃在 2 与 3 之间：批次被 checkpoint 覆盖但未发布，恢复时只做
            # settle（提交或隔离），绝不 abort——abort 后 source 重放会双发。
            self.sink.prepare(self._batch_id - 1, checkpoint=offset)

        ck = self.root / "_checkpoints"
        ck.mkdir(parents=True, exist_ok=True)
        meta = {
            "offset": offset,
            "batch_id": self._batch_id,
            "next_batch_id": self._batch_id,
            "watermark": self.wm.current.isoformat() if self.wm.current else None,
            "committed": self.two_phase,
        }
        _atomic_write_json(ck / f"chk-{self.stats.checkpoints:05d}.json", meta)
        self._resume_offset = offset

        if self.two_phase:
            self.sink.commit(self._batch_id - 1)

    # ---------- 维表 ----------

    def reload_dim(self, dim_rows: list[dict]) -> None:
        """全量 reload：先清空再写入。"""
        self.dim.reload(dim_rows)

    # ---------- 观测 ----------

    def snapshot(self) -> dict:
        """汇总落表结果。

        明细表是**事件粒度**（一行一个 event_id），因此：
            rows_written == 事件总数         → 不重不漏
            duplicate_rows == 0              → 没有因为重启重放而重复
        按订单口径另算一份"最终状态"，用于业务侧核对。
        """
        rows = self.sink.read_all()
        seen: dict[str, dict] = {}
        dups = 0
        for r in rows:
            eid = r["event_id"]
            if eid in seen:
                dups += 1
            seen[eid] = r

        # 订单最终状态：取 event_time 最新的那条
        by_order: dict[str, dict] = {}
        for r in rows:
            oid = r["order_id"]
            cur = by_order.get(oid)
            if cur is None or r["event_time"] > cur["event_time"]:
                by_order[oid] = r

        self.sink.stats.duplicates = dups
        return {
            "rows_written": len(rows),
            "distinct_events": len(seen),
            "duplicate_rows": dups,
            "distinct_orders": len(by_order),
            "files": self.sink.file_count(),
            "visible_batches": len(self.sink.visible_batch_ids()),
            "stats": {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict()},
            "dim": {"reloads": self.dim.reloads, "miss_during_reload": self.dim.miss_during_reload},
        }


def _window_date(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d")


def make_dim(n_shops: int = 40, seed: int = 7) -> list[dict]:
    import random

    rnd = random.Random(seed)
    regions = ["华东", "华南", "华北", "西南"]
    return [
        {
            "shop_id": f"SHOP-{i:03d}",
            "shop_name": f"{rnd.choice(regions)}店铺{i:02d}",
            "region": rnd.choice(regions),
            "tier": rnd.choice(["S", "A", "B"]),
        }
        for i in range(1, n_shops + 1)
    ]
