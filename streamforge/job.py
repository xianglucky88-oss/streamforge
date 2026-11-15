"""流式作业运行时：算子语义与 Flink 对齐。

关键概念在本文件里的对应关系：

    Flink 概念                 本实现
    -----------------------    ------------------------------------------
    Source                     iter(events)
    Watermark                   Watermarks 类，单调递增的下界
    KeyedState (ValueState)    DimTable 里的只读版本对象，TTL 到期即清除
    SideOutput (late data)     sink.late_events
    Checkpoint                 周期性把 offset 与维表版本 id 写进 _checkpoints/
    TwoPhaseCommitSink         2PC 开启时先写 staging 再原子提交
    Processing time            用 ingest_time 做窗口归属
    Event time                 用 event_time 做窗口归属

几个关键行为都在 `Job._consume` 与 `Job._checkpoint` 里：
    * 默认用 processing time（#2）→ 迟到数据被静默丢弃
    * 2PC 默认关闭（#1）→ 作业重启后重复写入
    * 维表 reload 采用双缓冲 + 原子版本切换（见 streamforge/dim.py）
    * 每批次一个文件（#4）→ 小文件爆炸
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import settings
from .dim import (
    DimLoadError,
    DimLoader,
    DimTable,
    DimVersion,
    DimVersionStore,
    DimVersionVerifyError,
    row_provenance_ok,
)
from .events import OrderEvent


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

# 维表已从"先清空再逐条写"改成双缓冲 + 原子版本切换，见 streamforge/dim.py。
# Job 只持有不变式：一条事件开始时取一次版本引用，整条事件都走该版本。


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

        # 重启时从磁盘恢复"已提交到哪一批"。
        # 已提交的 batch-*.jsonl 是事实，staging 目录里的东西属于上个事务，
        # 一律视为未提交（Flink 在 restore 时会 abort 掉 pending transaction）。
        existing = [int(p.stem.split("-")[1]) for p in self.root.glob("batch-*.jsonl")]
        self._committed_batch = (max(existing) + 1) if existing else 0
        self.stats.written = sum(_count_lines(p) for p in self.root.glob("batch-*.jsonl"))
        self.stats.files = len(existing)
        self.stats.commits = len(existing)
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
            self.stats.written += len(rows)
            self.stats.files += 1
            self.stats.commits += 1

    def commit(self, upto_batch: int) -> int:
        """把 staging 里 [next_commit, upto_batch) 的批次原子提交。返回提交批次数。"""
        if not self.two_phase:
            return 0
        n = 0
        for bid in range(self._committed_batch, upto_batch + 1):
            staged = self.root / "_staging" / f"batch-{bid:06d}.jsonl"
            if staged.exists():
                final = self.root / f"batch-{bid:06d}.jsonl"
                final.write_bytes(staged.read_bytes())
                self.stats.written += _count_lines(final)
                self.stats.files += 1
                self.stats.commits += 1
                n += 1
        self._committed_batch = upto_batch + 1
        return n

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
    # 本作业写出的行里出现过的维表版本数（用于证明"每条结果绑定了一个版本"）。
    dim_versions_used: set[str] = field(default_factory=set)

    def to_dict(self) -> dict:
        return {
            "consumed": self.consumed,
            "emitted": self.emitted,
            "late": self.late,
            "checkpoints": self.checkpoints,
            "restarts": self.restarts,
            "dim_misses": self.dim_misses,
            "dim_versions_used": len(self.dim_versions_used),
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
        dim_loader: DimLoader | None = None,
        dim_store: DimVersionStore | None = None,
        dim_max_bytes: int | None = None,
        checkpoint_interval: int | None = None,
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

        # 维表：版本仓库放在 sink 根目录下，和检查点一起参与恢复。
        self.dim_store = dim_store or DimVersionStore(self.root / "_dim_versions")
        loader = dim_loader or (lambda: iter(dim_rows))
        self.dim = DimTable(
            loader,
            store=self.dim_store,
            max_bytes=settings.dim_max_bytes if dim_max_bytes is None else dim_max_bytes,
            retain_versions=settings.dim_retain_versions,
            # 刷新在后台线程里构建；事件处理只做一次引用读取。
            background=True,
        )
        # 启动时同步发布首个版本；失败即启动失败，不存在"无版本可服务"的中间态。
        self.dim.load_blocking(loader, reason="startup")

        self.wm = Watermarks(allowed_lateness_seconds=self.allowed_lateness)
        self.stats = JobStats()
        # 检查点文件序号接续磁盘上已有的文件，重启不会覆盖历史检查点；
        # stats.checkpoints 只统计本次运行。
        self._ckpt_seq = len(list((self.root / "_checkpoints").glob("chk-*.json")))
        # 批次号从磁盘上已提交的批次之后继续，避免覆盖历史批次。
        self._batch_id = self.sink._committed_batch + self._pending_staging_batches()
        self._batch: list[dict] = []

    def _pending_staging_batches(self) -> int:
        """上一次事务留下但已被 abort 的批次号空洞数。

        恢复后为新数据分配新的 batch id，不复用被清掉的编号，
        这样"哪些文件是本次作业写的"在目录里依然可辨。
        """
        return 0

    # ---------- 主循环 ----------

    def run(
        self, events: list[OrderEvent], crash_at: int | None = None, idx_base: int = 0
    ) -> dict:
        """消费事件。

        crash_at: 处理到第 N 条（相对本次入参）时模拟进程被杀（不 commit、不 checkpoint）。
        idx_base: 本次消费的全局起始 offset，仅影响检查点触发位置。
        """
        reported: dict = {}
        try:
            for i, ev in enumerate(events):
                if crash_at is not None and i == crash_at:
                    raise CrashInjected(f"injected crash at event {idx_base + i}")
                self._consume(idx_base + i, ev)
        except CrashInjected as exc:
            self.stats.restarts += 1
            reported["crash"] = str(exc)
        self._flush()
        # 正常结束时补一次最终 commit。真实 Flink 在作业停止时也会做
        # 最后一次 checkpoint + commit；崩溃路径（上面的 except）不会走到这里，
        # 这正是 2PC 与"无 2PC"的差别所在。
        if "crash" not in reported and self.two_phase:
            self.sink.commit(self._batch_id - 1)
        return {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict(), **reported}

    def resume(self, events: list[OrderEvent], from_index: int) -> dict:
        """从指定 offset 继续消费。

        注意：因为 2PC 关闭，[from_index, crash_at) 之间已经写入的行
        会被再次写入。
        """
        return self.run(events[from_index:], idx_base=from_index)

    def resume_from_checkpoint(self, events: list[OrderEvent]) -> dict:
        """按最近一次检查点恢复：先对齐维表版本，再从 offset 继续。

        * 2PC：检查点前已 commit，按 offset 续跑即是 exactly-once；
        * 非 2PC：检查点前的批次可能尚未全部落盘，必须从 0 重放
          （at-least-once，会出现重复）。

        返回的 report 里带 ``resume_from`` 与 ``dim_restore``，便于审计。
        """
        checkpoint = self.latest_checkpoint()
        offset = int((checkpoint or {}).get("offset", 0) or 0)
        if not self.two_phase:
            offset = 0
        restore = self.restore_dim(checkpoint)
        report = self.resume(events, offset)
        return {**report, "resume_from": offset, "dim_restore": restore}

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
        else:
            # 处理时间模式下用 ingest_time，迟到数据在时间轴上完全正常，
            # 永远不会被判为 late，也不会被侧输出。
            pass

        # 取一次引用：整条事件（含所有维度字段）都用这一个版本，
        # 刷新线程在后台换指针不会让本事件前后读到不同内容。
        version = self.dim.current
        if version is None:
            raise DimLoadError("no active dim version")
        dim = version.lookup(ev.shop_id)
        self.dim.note_lookup(missed=dim is None)
        if dim is None:
            self.stats.dim_misses += 1
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
            # 维度版本溯源：每行都能证明自己用的是哪一份维表。
            "dim_version": version.id,
            "dim_digest": version.digest,
        }
        self._batch.append(row)
        self.stats.emitted += 1
        self.stats.dim_versions_used.add(version.id)

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
        # 2PC 下先落批再提交：检查点 offset 与"已提交数据"严格对齐，
        # 按 offset 恢复时不会丢掉检查点之前仍在内存里的批次。
        if self.two_phase:
            self._flush()
        self.stats.checkpoints += 1
        self._ckpt_seq += 1
        ck = self.root / "_checkpoints"
        ck.mkdir(parents=True, exist_ok=True)
        meta = {
            "offset": offset,
            "batch_id": self._batch_id,
            "watermark": self.wm.current.isoformat() if self.wm.current else None,
            "committed": self.two_phase,
            # 检查点绑定维表版本：恢复时按 id 从版本仓库取回同一份内容，
            # 而不是"重启后用最新维表重算历史"。
            "dim_version": self.dim.active_id,
            "dim_digest": self.dim.active_digest,
        }
        (ck / f"chk-{self._ckpt_seq:05d}.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if self.two_phase:
            self.sink.commit(self._batch_id - 1)

    def latest_checkpoint(self) -> dict | None:
        ck = self.root / "_checkpoints"
        files = sorted(ck.glob("chk-*.json"))
        if not files:
            return None
        try:
            return json.loads(files[-1].read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def restore_dim(self, checkpoint: dict | None = None) -> dict:
        """按检查点记录的版本 id 恢复维表。

        恢复失败（快照缺失/损坏/超预算）时保留启动时的版本，并记一次
        ``restore_failures``；后台刷新仍会在之后尝试切换，因此不会卡死。
        """
        meta = checkpoint if checkpoint is not None else self.latest_checkpoint()
        version_id = (meta or {}).get("dim_version")
        if not version_id:
            return {"restored": False, "reason": "checkpoint has no dim_version"}
        expected = (meta or {}).get("dim_digest")
        if expected is not None:
            # 先校验再切换：不允许把一个"看起来对、内容不对"的版本发布出去。
            try:
                on_disk = self.dim_store.get(str(version_id))
            except DimVersionVerifyError as exc:
                return {"restored": False, "version": version_id, "reason": str(exc)}
            if on_disk.digest != expected:
                return {
                    "restored": False,
                    "version": version_id,
                    "reason": "digest differs from checkpoint",
                }
        try:
            v = self.dim.restore(str(version_id), reason="checkpoint-restore")
        except (DimVersionVerifyError, DimLoadError) as exc:
            return {"restored": False, "version": version_id, "reason": str(exc)}
        return {"restored": True, "version": v.id, "digest": v.digest}

    # ---------- 维表 ----------

    def reload_dim(self, dim_rows: list[dict] | None = None) -> DimVersion:
        """发布一个新版本（后台刷新入口之一）。

        默认后台构建：事件循环不被加载阻塞；传入 ``rows`` 时同样先构建完整
        快照再一次性换指针。返回请求前正在服务的版本。
        """
        loader = None if dim_rows is None else (lambda: iter(dim_rows))
        active = self.dim.current
        if loader is not None:
            self.dim.request_refresh(loader)
        else:
            self.dim.request_refresh()
        return active  # 新版本就绪后原子生效；调用方不会被加载阻塞

    def refresh_dim_blocking(self, loader: DimLoader | None = None) -> DimVersion:
        """同步刷新，仅供测试/显式维护入口。"""
        return self.dim.load_blocking(loader)

    def rollback_dim(self, version_id: str | None = None) -> DimVersion:
        """原子回退到上一版本（或指定版本）。回退前的行仍绑定原版本 id。"""
        return self.dim.rollback(version_id)

    def close(self, timeout: float | None = 5.0) -> None:
        """停止后台刷新线程（在途加载会被等完）。"""
        self.dim.close(timeout)

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
        dim_report = self.dim.report()
        # 落到结果行的版本必须都在版本仓库里可读且内容吻合。
        provenance = self.verify_dim_provenance(rows)
        return {
            "rows_written": len(rows),
            "distinct_events": len(seen),
            "duplicate_rows": dups,
            "distinct_orders": len(by_order),
            "files": self.sink.file_count(),
            "stats": {"job": self.stats.to_dict(), "sink": self.sink.stats.to_dict()},
            "dim": {
                **{k: v for k, v in dim_report.items() if k != "reloads"},
                # 旧快照兼容字段：成功切换次数。
                "reloads": dim_report["reloads"],
                "row_versions": provenance["versions"],
                "provenance_checked": provenance["checked"],
                "provenance_failures": provenance["failures"],
            },
        }

    def verify_dim_provenance(self, rows: list[dict] | None = None) -> dict:
        """逐行证明关联字段来自其声明的维度版本。"""
        rows = self.sink.read_all() if rows is None else rows
        versions: dict[str, int] = {}
        failures: list[dict] = []
        cache: dict[str, DimVersion | str] = {}  # 版本按 id 读一次，避免逐行读盘
        for r in rows:
            vid = r.get("dim_version")
            versions[vid] = versions.get(vid, 0) + 1
            if vid not in cache:
                if not vid:
                    cache[vid] = "missing dim_version"
                else:
                    try:
                        cache[vid] = self.dim_store.get(str(vid))
                    except DimVersionVerifyError as exc:
                        cache[vid] = str(exc)
            resolved = cache[vid]
            if isinstance(resolved, str):
                ok, why = False, resolved
            else:
                ok, why = row_provenance_ok(r, version=resolved)
            if not ok:
                failures.append({"event_id": r.get("event_id"), "dim_version": vid, "why": why})
        return {"checked": len(rows), "failures": failures, "versions": versions}


class CrashInjected(RuntimeError):
    """测试用的崩溃信号。"""


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
