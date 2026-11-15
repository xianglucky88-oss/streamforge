"""流式作业运行时：算子语义与 Flink 对齐。

关键概念在本文件里的对应关系：

    Flink 概念                 本实现
    -----------------------    ------------------------------------------
    Source                     iter(events)
    Watermark                   Watermarks 类，单调递增的下界
    KeyedState (ValueState)    DimState，TTL 到期即清除
    SideOutput (late data)     sink.late_events
    Checkpoint                 周期性把 offset 写进 _checkpoints/
    TwoPhaseCommitSink         2PC 开启时先写 staging 再原子提交
    Processing time            用 ingest_time 做窗口归属
    Event time                 用 event_time 做窗口归属

几个关键行为都在 `Job._consume` 与 `Job._checkpoint` 里：
    * 默认用 processing time（#2）→ 迟到数据被静默丢弃
    * 2PC 默认关闭（#1）→ 作业重启后重复写入
    * 维表 reload 采用全量覆盖
    * 每批次一个文件（#4）→ 小文件爆炸
"""

from __future__ import annotations

import json
import os
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .config import settings
from .events import OrderEvent
from .poison import (
    DeadLetterQueue,
    DeadLetterSink,
    DecodedMessage,
    DecodeError,
    RawMessage,
    decode_message,
    make_dead_letter,
    message_fingerprint,
)


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
        }


class Sink:
    """Iceberg sink 的文件级模拟。

    每个 batch 写一个文件；2PC 开启时先写 staging 目录，
    checkpoint 触发时才 move 到正式目录（这里用"写标记文件"模拟原子 rename）。
    """

    def __init__(self, root: Path, two_phase: bool) -> None:
        self.root = root
        self.two_phase = two_phase
        self.stats = SinkStats()
        self.root.mkdir(parents=True, exist_ok=True)
        (root / "_staging").mkdir(exist_ok=True)

        # 已提交批次以**编号集合**为准，而不是连续高水位：
        # 崩溃恢复后新批次的编号可能高于尚未提交的旧编号（abort 留下空洞），
        # 高水位会把这些新批次错判成"已提交"而在 commit 时静默跳过 → 丢数据。
        # 磁盘上的 batch-*.jsonl 就是提交事实，启动时据此重建集合。
        existing = [int(p.stem.split("-")[1]) for p in self.root.glob("batch-*.jsonl")]
        self.committed_ids: set[int] = set(existing)
        self.stats.written = sum(_count_lines(p) for p in self.root.glob("batch-*.jsonl"))
        self.stats.files = len(existing)
        self.stats.commits = len(existing)
        # 下一个批次编号必须越过所有已提交编号和被 abort 的 staging 编号，
        # 既不覆盖历史文件，也不复用悬空编号。
        staged_ids = [
            int(p.stem.split("-")[1])
            for p in (self.root / "_staging").glob("batch-*.jsonl")
        ]
        self.next_batch_id = max([*existing, *staged_ids], default=-1) + 1
        # staging 目录里的东西属于上个事务，一律视为未提交
        # （Flink 在 restore 时会 abort 掉 pending transaction）。
        for staged in (self.root / "_staging").glob("batch-*.jsonl"):
            staged.unlink(missing_ok=True)

    def write_batch(self, batch_id: int, rows: list[dict]) -> None:
        name = f"batch-{batch_id:06d}.jsonl"
        if self.two_phase:
            staged = self.root / "_staging" / name
            _write_rows(staged, rows)
            self.stats.staging_writes += len(rows)
        else:
            # 未启用 2PC 时直接写正式目录。
            # 作业在这之后崩溃 → 重启从上一个 checkpoint 重放 → 重复写入。
            final = self.root / name
            _write_rows(final, rows)
            self.committed_ids.add(batch_id)
            self.stats.written += len(rows)
            self.stats.files += 1
            self.stats.commits += 1

    def commit_pending(self) -> list[int]:
        """把 staging 里所有尚未提交的批次原子落盘。

        按编号集合判定，天然容忍 abort 空洞与重启后的编号跳跃。
        返回本次实际提交的批次编号；调用方据此决定是否提升去重状态。
        """
        if not self.two_phase:
            return []
        committed_now: list[int] = []
        for staged in sorted((self.root / "_staging").glob("batch-*.jsonl")):
            bid = int(staged.stem.split("-")[1])
            if bid in self.committed_ids:
                # 已提交过（重复 commit 调用），跳过，保证幂等。
                continue
            final = self.root / staged.name
            final.write_bytes(staged.read_bytes())
            self.committed_ids.add(bid)
            self.stats.written += _count_lines(final)
            self.stats.files += 1
            self.stats.commits += 1
            committed_now.append(bid)
        return committed_now

    # 兼容旧调用方与测试的高水位视图（仅用于展示，不参与提交判定）。
    @property
    def _committed_batch(self) -> int:
        return (max(self.committed_ids) + 1) if self.committed_ids else 0

    def read_all(self) -> list[dict]:
        rows: list[dict] = []
        for p in sorted(self.root.glob("batch-*.jsonl")):
            rows.extend(_read_rows(p))
        return rows

    def file_count(self) -> int:
        return len(list(self.root.glob("batch-*.jsonl")))


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
    # 原始消息入口（run_raw）的隔离与去重计数
    raw_in: int = 0
    dead_lettered: int = 0
    duplicates: int = 0
    poison_skipped: int = 0

    def to_dict(self) -> dict:
        return {
            "consumed": self.consumed,
            "emitted": self.emitted,
            "late": self.late,
            "checkpoints": self.checkpoints,
            "restarts": self.restarts,
            "dim_misses": self.dim_misses,
            "raw_in": self.raw_in,
            "dead_lettered": self.dead_lettered,
            "duplicates": self.duplicates,
            "poison_skipped": self.poison_skipped,
        }


class DedupRegistry:
    """事件去重台账，随 checkpoint 持久化，重启后继续生效。

    Kafka 在位移提交前重平衡 / 进程重启会原样重放消息；2PC 只保证
    sink 端不重复提交，但去重发生在算子入口，需要一份独立状态。
    语义对应 Flink 里随 checkpoint 持久化的 ``MapState<event_id, ...>``。

    状态分两层，与 2PC 事务边界严格对齐：

    * ``_pending``：本进程见过、但对应数据**尚未确认提交**的 event_id。
      进程内重投递（同一次运行里的重复消息）由它挡住。
    * ``_committed``：已随成功的 checkpoint / sink commit 落盘的 event_id，
      重启后从磁盘恢复。

    只有 sink 提交成功后才调用 :meth:`promote` 把 pending 并入 committed 并
    原子写盘。若在提交前崩溃，pending 随进程消失，重启重放时这些消息会被
    正常重新处理——与 2PC 中止 staging 后重放的口径一致，绝不丢数据。
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._committed: set[str] = set()
        self._pending: set[str] = set()
        self._reload()

    def _reload(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # 状态文件损坏时按空集合启动，宁可重复也不阻塞作业。
            return
        if isinstance(data, list):
            self._committed = {eid for eid in data if isinstance(eid, str)}

    def is_duplicate(self, event_id: str) -> bool:
        return event_id in self._committed or event_id in self._pending

    def remember(self, event_id: str) -> None:
        """标记一条已处理消息。仅进 pending，提交成功后才持久化。"""
        self._pending.add(event_id)

    def promote(self) -> int:
        """把 pending 并入 committed 并原子落盘。返回新提升的条数。

        必须在 sink commit **成功之后**调用，保证"已去重"与"已落表"
        这两个事实同生共死。
        """
        new = self._pending - self._committed
        if not new:
            return 0
        self._committed |= self._pending
        self._pending.clear()
        self._checkpoint()
        return len(new)

    def _checkpoint(self) -> None:
        """原子写快照：先写临时文件再 rename，避免崩溃留下半截状态。"""
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(sorted(self._committed), ensure_ascii=False), encoding="utf-8"
        )
        os.replace(tmp, self.path)

    def __len__(self) -> int:
        return len(self._committed) + len(self._pending)


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
        checkpoint_interval: int | None = None,
        dlq_downstream: DeadLetterSink | None = None,
    ) -> None:
        self.root = root or settings.sink_root
        settings.ensure()
        self.two_phase = settings.two_phase_commit if two_phase is None else two_phase
        self.processing_time = settings.processing_time if processing_time is None else processing_time
        self.allowed_lateness = (
            settings.allowed_lateness_seconds if allowed_lateness is None else allowed_lateness
        )
        self.batch_size = settings.batch_size if batch_size is None else batch_size
        self.checkpoint_interval = (
            settings.checkpoint_interval if checkpoint_interval is None else checkpoint_interval
        )

        self.sink = Sink(self.root / "orders", two_phase=self.two_phase)
        self.late_sink = self.root / "late_events"
        self.late_sink.mkdir(parents=True, exist_ok=True)
        # 坏消息隔离：DLQ 落盘 + 毒丸台账 + 下游故障溢出，均在 root 下持久化。
        self.dlq = DeadLetterQueue(self.root, downstream=dlq_downstream)
        # 去重台账：重启后从最近一次 checkpoint 恢复已见 event_id。
        self.dedup = DedupRegistry(self.root / "_checkpoints" / "seen_event_ids.json")
        self.dim = DimState()
        self.dim.reload(dim_rows)
        self.wm = Watermarks(allowed_lateness_seconds=self.allowed_lateness)
        self.stats = JobStats()
        # 批次号接着 sink 恢复出的"下一个可用编号"走：既不覆盖已提交文件，
        # 也不复用被 abort 的 staging 编号（复用会在 commit 时静默丢批）。
        self._batch_id = self.sink.next_batch_id
        self._batch: list[dict] = []

    # ---------- 主循环 ----------

    def run(self, events: list[OrderEvent], crash_at: int | None = None) -> dict:
        """消费事件。

        crash_at: 处理到第 N 条时模拟进程被杀（不 commit、不 checkpoint）。
        """
        reported: dict = {}
        try:
            for i, ev in enumerate(events):
                if crash_at is not None and i == crash_at:
                    raise CrashInjected(f"injected crash at event {i}")
                self._consume(i, ev)
        except CrashInjected as exc:
            self.stats.restarts += 1
            reported["crash"] = str(exc)
        self._flush()
        # 正常结束时补一次最终 commit。真实 Flink 在作业停止时也会做
        # 最后一次 checkpoint + commit；崩溃路径（上面的 except）不会走到这里，
        # 这正是 2PC 与"无 2PC"的差别所在。
        if "crash" not in reported:
            if self.two_phase:
                self.sink.commit_pending()
            self._finalize()
        return {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict(), **reported}

    def run_raw(self, messages: list[RawMessage], crash_at: int | None = None) -> dict:
        """消费原始消息（截断 / 未知版本 / 类型漂移都可能混在里面）。

        与 :meth:`run` 的差别在入口：每条消息先过"毒丸台账 → 解码契约校验
        → 事件去重"三道隔离，任何一道不通过都不会进入正常窗口，
        也不会让异常冒泡终止作业。

        crash_at: 处理到第 N 条原始消息时模拟进程被杀。
        """
        reported: dict = {}
        try:
            for i, msg in enumerate(messages):
                if crash_at is not None and i == crash_at:
                    raise CrashInjected(f"injected crash at raw message {i}")
                self._consume_raw(i, msg)
        except CrashInjected as exc:
            self.stats.restarts += 1
            reported["crash"] = str(exc)
        self._flush()
        if "crash" not in reported:
            if self.two_phase:
                self.sink.commit_pending()
            self._finalize()
        return {
            "job": self.stats.to_dict(),
            "sink": self.sink.stats.to_dict(),
            "dlq": self.dlq.stats.to_dict(),
            **reported,
        }

    def resume(self, events: list[OrderEvent], from_index: int) -> dict:
        """从上次 checkpoint 的位置继续消费。

        注意：因为 2PC 关闭，[from_index, crash_at) 之间已经写入的行
        会被再次写入。
        """
        return self.run(events[from_index:])

    def _finalize(self) -> None:
        """正常收尾：去重状态提升到已提交，并补投此前溢出的死信。"""
        self.dedup.promote()
        self.dlq.drain()

    def _consume(self, idx: int, ev: OrderEvent) -> None:
        # 结构化事件入口：事件已由上游解码，直接计入消费并处理。
        self.stats.consumed += 1
        self._process_valid_event(idx, ev)

    def _consume_raw(self, idx: int, msg: RawMessage) -> None:
        self.stats.raw_in += 1

        # 第一道：毒丸台账。重启后同一条毒丸被重投递时在此直接跳过，
        # 不再解析、不再写 DLQ，从根上断掉"崩溃→重放→崩溃"的循环。
        fingerprint = message_fingerprint(msg.topic, msg.partition, msg.offset, msg.raw)
        if self.dlq.ledger.is_poison(fingerprint):
            self.dlq.ledger.note_redelivery(fingerprint)
            self.stats.poison_skipped += 1
            return

        # 第二道：反序列化 + 版本 + 契约。失败转死信，异常不外溢。
        try:
            decoded = decode_message(msg)
        except DecodeError as exc:
            self.dlq.accept(make_dead_letter(msg, exc))
            self.stats.dead_lettered += 1
            return

        # 第三道：事件去重。重投递的合法消息不允许产生第二行。
        event_id = decoded.event_id
        if event_id is not None and self.dedup.is_duplicate(event_id):
            self.stats.duplicates += 1
            return
        if event_id is not None:
            self.dedup.remember(event_id)

        # 通过全部隔离的消息才计入正常消费并进入窗口处理。
        self.stats.consumed += 1
        self._process_valid_event(idx, _event_from_payload(decoded))

    def _process_valid_event(self, idx: int, ev: OrderEvent) -> None:
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
        else:
            # 处理时间模式下用 ingest_time，迟到数据在时间轴上完全正常，
            # 永远不会被判为 late，也不会被侧输出。
            pass

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

        if (idx + 1) % self.checkpoint_interval == 0:
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

    def _checkpoint(self, offset: int) -> None:
        self.stats.checkpoints += 1
        ck = self.root / "_checkpoints"
        ck.mkdir(parents=True, exist_ok=True)
        meta = {
            "offset": offset,
            "batch_id": self._batch_id,
            "watermark": self.wm.current.isoformat() if self.wm.current else None,
            "committed": self.two_phase,
        }
        (ck / f"chk-{self.stats.checkpoints:05d}.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # 2PC 关闭时，checkpoint 之前的数据已经落盘；
        # 2PC 开启时，先让 sink 成功提交，再提升去重状态——
        # 保证"已去重"与"已落表"同生共死：只有本次确实有批次提交成功，
        # 才把对应的 pending event_id 落盘；提交前崩溃则两者一起回滚重放。
        if self.two_phase:
            committed_now = self.sink.commit_pending()
            if committed_now:
                self.dedup.promote()
        else:
            self.dedup.promote()
        # 下游此前不可用而溢出的死信，借 checkpoint 周期补投。
        self.dlq.drain()

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
        letters = self.dlq.read_all()
        return {
            "rows_written": len(rows),
            "distinct_events": len(seen),
            "duplicate_rows": dups,
            "distinct_orders": len(by_order),
            "files": self.sink.file_count(),
            "stats": {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict()},
            "dim": {"reloads": self.dim.reloads, "miss_during_reload": self.dim.miss_during_reload},
            # 死信口径：坏消息的去向在这里可审计，正常窗口不被污染。
            "dead_letters": {
                "count": len(letters),
                "spilled": self.dlq.spill_count(),
                "stats": self.dlq.stats.to_dict(),
                "poison_ledger": len(self.dlq.ledger),
            },
        }


class CrashInjected(RuntimeError):
    """测试用的崩溃信号。"""


def _event_from_payload(decoded: DecodedMessage) -> OrderEvent:
    """通过契约校验的负载转结构化事件；schema_version 等额外字段被忽略。"""
    p = decoded.payload
    return OrderEvent(
        event_id=p["event_id"],
        order_id=p["order_id"],
        shop_id=p["shop_id"],
        status=p["status"],
        amount_cents=p["amount_cents"],
        channel=p["channel"],
        event_time=p["event_time"],
        ingest_time=p["ingest_time"],
    )


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
