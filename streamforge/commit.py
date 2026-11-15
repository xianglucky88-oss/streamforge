"""端到端原子提交协议：暂存 → 预提交 → 检查点确认 → 正式发布 → 异常清理。

背景
====
旧实现把数据先放进 ``_staging/``，checkpoint 时用 ``read_bytes + write_bytes``
模拟 rename，恢复时无条件删除 staging。它有四个无法自愈的缺口：

1. 进程在"数据已写、标记未落"之间终止 → 状态无法判定，旧逻辑一律按未提交处理，
   但对象存储上的写入可能已经生效，重放会造成双发。
2. 对象存储的 list / 跨 key 读取存在延迟可见：刚写的标记与日志条目可能暂时
   list 不出来，基于目录扫描的决策会把"已提交"误判成"未提交"。
3. 没有持久化的提交事实：下游直接 glob 数据文件，发布过程中会读到半个批次；
   恢复逻辑也无从区分已提交 / 未完成 / 无法判定。
4. 清理与恢复没有互斥与代际（epoch）fencing：并发清理可能删掉别人正在提交的
   事务目录，重复恢复可能重复发布。

本模块给出的协议
================
每个批次是一个**事务目录**，事务状态由标记文件显式表达，状态机为：

    PENDING ──prepare──▶ PREPARED ──commit──▶ COMMITTED
       │                    │
       └────────abort──────┴──────────────▶ ABORTED
                       （无法判定时）──────▶ QUARANTINED

* 数据文件只写一次、永不改名、永不就地修改（对象存储没有原子 rename）。
* "正式发布"= 向**只追加的提交日志（journal）**追加一条 commit 条目，再翻转
  CURRENT 指针。下游只通过 journal 读取，因此要么看到整个批次、要么完全看不到，
  发布中途崩溃对下游天然不可见，也不存在读到半个文件的竞争。
* journal 追加按 ``put_if_absent(序号 key)`` 去重，重复提交同一事务是幂等的。
* 恢复时按"标记 + journal + 最近一次 checkpoint"三源对账，明确输出
  已提交 / 未完成（abort）/ 无法判定（隔离区）三类结论；隔离区保留数据、
  永久占用该批次号，绝不依赖人工删文件来维持一致性。
* 恢复与清理共用一把带租约与 epoch fencing 的锁，重复恢复与并发清理都是安全的。

对象存储语义由 :class:`ObjectStore` 抽象承载，
:class:`LocalObjectStore` 用本地文件系统模拟了延迟可见、写入结果不确定与
进程硬终止三类故障，便于确定性测试。
"""

from __future__ import annotations

import json
import os
import threading
import uuid
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable


# ---------------------------------------------------------------- 故障信号


class CrashInjected(RuntimeError):
    """模拟进程被 SIGKILL：调用方没有机会写任何善后状态。"""


class StoreIndeterminate(RuntimeError):
    """写入请求的结局无法判定（超时 / 5xx / 连接断开）。

    真实对象存储上这意味着"可能写成功了，也可能没有"，协议必须通过
    重新 list / get 来确认，而不是盲目重写或放弃。
    """


# ---------------------------------------------------------------- 对象存储


class ObjectStore:
    """对象存储接口。刻意不提供 rename：对象存储的跨 key 移动不是原子的。

    可见性语义（对标 S3）：

    * ``put`` 成功返回后，单 key 的 read-after-write 通常立即可见；
    * 但 list-bucket 以及"别的进程发起的读"可能延迟可见；
    * 本实现用一个单调的可见性时钟统一建模，``tick()`` 推进一步，
      协调器在所有重试循环里 tick，因此延迟只会造成重试，不会造成误判。
    """

    def put(self, key: str, data: bytes) -> None:
        raise NotImplementedError

    def put_if_absent(self, key: str, data: bytes) -> bool:
        """条件写：key 已存在（即使尚未可见）时返回 False，不覆盖。"""
        raise NotImplementedError

    def get(self, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        raise NotImplementedError

    def list(self, prefix: str = "") -> list[str]:
        raise NotImplementedError

    def delete(self, key: str) -> None:
        raise NotImplementedError

    def tick(self) -> None:
        """推进一步可见性时钟。"""
        raise NotImplementedError


class LocalObjectStore(ObjectStore):
    """本地文件系统版对象存储，带三类可注入故障。

    * ``visibility_lag_ticks``：写入后经过多少个 tick 才对 list/get 可见。
    * ``fail_on``：key 命中任一 token 的 put 抛 :class:`StoreIndeterminate`；
      ``fail_durable`` 控制这次不确定写入是否已经物理落盘。
    * ``crash_on``：key 命中任一 token 的 put 直接抛 :class:`CrashInjected`，
      模拟硬终止（任何善后代码都没机会执行）。
    """

    def __init__(
        self,
        root: Path,
        *,
        visibility_lag_ticks: int = 0,
        fail_on: Iterable[str] = (),
        crash_on: Iterable[str] = (),
        fail_durable: bool = False,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.visibility_lag_ticks = visibility_lag_ticks
        self.fail_on = tuple(fail_on)
        self.crash_on = tuple(crash_on)
        self.fail_durable = fail_durable
        self._clock = 0
        self._visible_at: dict[str, int] = {}
        self._lock = threading.RLock()

        # 进程启动时，目录里已有的文件一律视为立即可见（上次进程的遗产）。
        for path in self.root.rglob("*"):
            if path.is_file() and not path.name.startswith("."):
                key = path.relative_to(self.root).as_posix()
                self._visible_at[key] = 0

    def tick(self) -> None:
        with self._lock:
            self._clock += 1

    def _physical_path(self, key: str) -> Path:
        p = (self.root / key).resolve()
        if not str(p).startswith(str(self.root.resolve())):
            raise ValueError(f"key escapes store root: {key}")
        return p

    def _is_visible(self, key: str) -> bool:
        return key in self._visible_at and self._clock >= self._visible_at[key]

    def put(self, key: str, data: bytes) -> None:
        with self._lock:
            if any(token in key for token in self.crash_on):
                raise CrashInjected(f"hard crash while writing {key}")

            durable = True
            if any(token in key for token in self.fail_on):
                durable = self.fail_durable

            if durable:
                path = self._physical_path(key)
                path.parent.mkdir(parents=True, exist_ok=True)
                # 先写临时文件再原子替换：进程在替换前终止不会留下半个对象。
                tmp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex[:8])
                tmp.write_bytes(data)
                os.replace(tmp, path)
                self._visible_at[key] = self._clock + self.visibility_lag_ticks

            if any(token in key for token in self.fail_on):
                raise StoreIndeterminate(f"write outcome unknown for {key}")

    def put_if_absent(self, key: str, data: bytes) -> bool:
        with self._lock:
            physical = self._physical_path(key)
            if physical.exists() or key in self._visible_at:
                return False
            self.put(key, data)
            return True

    def get(self, key: str) -> bytes:
        with self._lock:
            if not self._is_visible(key):
                raise KeyError(key)
            return self._physical_path(key).read_bytes()

    def exists(self, key: str) -> bool:
        with self._lock:
            return self._is_visible(key)

    def list(self, prefix: str = "") -> list[str]:
        with self._lock:
            return sorted(
                k
                for k in self._visible_at
                if self._is_visible(k) and k.startswith(prefix)
            )

    def delete(self, key: str) -> None:
        with self._lock:
            self._physical_path(key).unlink(missing_ok=True)
            self._visible_at.pop(key, None)
            # 顺手清理空目录，绝不删到 store 根。
            parent = self._physical_path(key).parent
            while parent != self.root.resolve():
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent


# ---------------------------------------------------------------- 状态机


class TxnState(str, Enum):
    PENDING = "pending"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"
    QUARANTINED = "quarantined"


MARKER_PREPARED = "prepared.json"
MARKER_COMMITTED = "committed.json"
MARKER_ABORTED = "aborted.json"

TXN_PREFIX = "_txn/"
JOURNAL_PREFIX = "_journal/"
CURRENT_KEY = "_journal/CURRENT"
QUARANTINE_PREFIX = "_quarantine/"
LOCK_KEY = "_recovery.lock"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonb(obj: dict) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


@dataclass
class TxnHandle:
    """一个批次事务的内存句柄。"""

    txn_id: str
    batch_id: int
    data_key: str
    state: TxnState = TxnState.PENDING
    row_count: int = 0
    checksum: int = 0


@dataclass
class JournalEntry:
    """只追加日志里的一条记录。commit 发布、abort/隔离留墓碑。"""

    seq: int
    kind: str
    txn_id: str
    batch_id: int
    files: list[str]
    row_count: int
    checksum: int
    checkpoint: int | None
    created_at: str

    def to_json(self) -> bytes:
        return json.dumps(
            {
                "seq": self.seq,
                "kind": self.kind,
                "txn_id": self.txn_id,
                "batch_id": self.batch_id,
                "files": self.files,
                "row_count": self.row_count,
                "checksum": self.checksum,
                "checkpoint": self.checkpoint,
                "created_at": self.created_at,
            },
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, d: dict) -> "JournalEntry":
        return cls(
            seq=int(d["seq"]),
            kind=d["kind"],
            txn_id=d["txn_id"],
            batch_id=int(d["batch_id"]),
            files=list(d["files"]),
            row_count=int(d["row_count"]),
            checksum=int(d.get("checksum", 0)),
            checkpoint=d.get("checkpoint"),
            created_at=d["created_at"],
        )


@dataclass
class RecoveryReport:
    """一次恢复对账的明确结论。三类批次互不重叠。"""

    committed: list[int] = field(default_factory=list)
    aborted: list[int] = field(default_factory=list)
    quarantined: list[int] = field(default_factory=list)
    indeterminate_seen: int = 0
    journal_repaired: bool = False
    lock_epoch: int = 0
    resume_offset: int | None = None

    def to_dict(self) -> dict:
        return {
            "committed": self.committed,
            "aborted": self.aborted,
            "quarantined": self.quarantined,
            "indeterminate_seen": self.indeterminate_seen,
            "journal_repaired": self.journal_repaired,
            "lock_epoch": self.lock_epoch,
            "resume_offset": self.resume_offset,
        }


@dataclass
class CoordinatorStats:
    txns_begun: int = 0
    rows_staged: int = 0
    prepared: int = 0
    committed: int = 0
    aborted: int = 0
    quarantined: int = 0
    reconciled_deletes: int = 0
    visibility_retries: int = 0
    indeterminate_writes: int = 0

    def to_dict(self) -> dict:
        return dict(self.__dict__)


# ---------------------------------------------------------------- 恢复锁


class _RecoveryLock:
    """带租约与 epoch 的恢复锁。

    * 同一进程内：可重入的线程锁，保证并发清理 / 恢复串行化。
    * 跨进程 / 跨代：文件锁 ``put_if_absent`` 获取；持有者周期性续租，
      租约过期可被新协调器接管，接管时 epoch +1，旧持有者的后续动作
      通过 epoch 校验被拒绝（fencing）。
    """

    def __init__(self, store: ObjectStore, lease_seconds: float = 30.0) -> None:
        self._store = store
        self._lease_seconds = lease_seconds
        self._thread_lock = threading.RLock()
        self.owner: str | None = None
        self.epoch = 0
        self.acquired_at_clock: int | None = None

    def acquire(self) -> "_RecoveryLock":
        self._thread_lock.acquire()
        token = uuid.uuid4().hex

        existing = self._read_with_retries()
        if existing is not None and existing["owner"] != self.owner:
            if self._wallnow() < float(existing.get("leased_until_wall", 0)):
                self._thread_lock.release()
                raise RuntimeError("recovery lock held by another live coordinator")
            # 租约已过期：接管前先继承旧代际，保证 epoch 单调递增。
            self.epoch = int(existing.get("epoch", self.epoch))
            existing = None

        if existing is None:
            self._store.delete(LOCK_KEY)
            payload = {
                "owner": token,
                "epoch": self.epoch + 1,
                "leased_until_wall": self._wallnow() + self._lease_seconds,
                "acquired_at": _utcnow(),
            }
            if not self._store.put_if_absent(LOCK_KEY, _jsonb(payload)):
                other = self._read_with_retries()
                if other is None or other["owner"] == token:
                    payload = other or payload
                else:
                    self._thread_lock.release()
                    raise RuntimeError("lost recovery-lock race")
            self.owner = payload["owner"]
            self.epoch = payload["epoch"]
        else:
            # 可重入：同一持有者再次进入，续租即可。
            self.owner = existing["owner"]
            self.epoch = existing["epoch"]
            self._heartbeat()

        self.acquired_at_clock = getattr(self._store, "_clock", 0)
        return self

    @staticmethod
    def _wallnow() -> float:
        return datetime.now(timezone.utc).timestamp()

    def _read_with_retries(self, attempts: int = 5) -> dict | None:
        for _ in range(attempts):
            try:
                return json.loads(self._store.get(LOCK_KEY))
            except KeyError:
                # 不可见就 tick 推进可见性时钟，而不是立刻相信"锁不存在"。
                self._store.tick()
        return None

    def _heartbeat(self) -> None:
        payload = {
            "owner": self.owner,
            "epoch": self.epoch,
            "leased_until_wall": self._wallnow() + self._lease_seconds,
            "renewed_at": _utcnow(),
        }
        self._store.put(LOCK_KEY, _jsonb(payload))

    def check_fence(self) -> None:
        """提交 / 隔离动作前调用：锁被新一代接管则拒绝继续。"""
        current = self._read_with_retries()
        if current is not None and current["owner"] != self.owner:
            raise RuntimeError("fenced: recovery lock epoch advanced")

    def release(self) -> None:
        try:
            current = self._read_with_retries(3)
            if current is not None and current["owner"] == self.owner:
                self._store.delete(LOCK_KEY)
        finally:
            self._thread_lock.release()

    def __enter__(self) -> "_RecoveryLock":
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


# ---------------------------------------------------------------- 协调器


class CommitCoordinator:
    """原子提交协调器：所有状态都落在 ObjectStore 上，进程身份不持久。"""

    def __init__(
        self,
        store: ObjectStore,
        *,
        table: str = "orders",
        visibility_attempts: int = 10,
    ) -> None:
        self.store = store
        self.table = table
        self.visibility_attempts = visibility_attempts
        self.stats = CoordinatorStats()
        self._lock = _RecoveryLock(store)
        # txn_id -> TxnHandle（仅本进程内的活动事务）。
        self._txns: dict[str, TxnHandle] = {}
        self._journal: list[JournalEntry] = []
        self._committed_txn_ids: set[str] = set()
        self._aborted_txn_ids: set[str] = set()
        self._reload_journal()

    # ---------- journal 投影 ----------

    def _reload_journal(self) -> None:
        """从存储重建 journal 投影，容忍 CURRENT 滞后 / 不可见。"""
        keys = self._list_visible(JOURNAL_PREFIX)
        entry_keys = sorted(k for k in keys if k.startswith(JOURNAL_PREFIX + "entry-"))
        entries: list[JournalEntry] = []
        for k in entry_keys:
            data = self._get_with_visibility(k)
            entries.append(JournalEntry.from_dict(json.loads(data)))
        entries.sort(key=lambda e: e.seq)
        self._journal = entries
        self._committed_txn_ids = {e.txn_id for e in entries if e.kind == "commit"}
        self._aborted_txn_ids = {e.txn_id for e in entries if e.kind == "abort"}

    def _next_journal_seq(self) -> int:
        return max((e.seq for e in self._journal), default=0) + 1

    def _append_journal(self, entry: JournalEntry) -> bool:
        """幂等追加：同 seq / 同 txn 的条目已存在则跳过。

        返回 True 表示本次实际写入。``put_if_absent`` 保证并发恢复下
        序号 key 不会被两个协调器同时占用。
        """
        if entry.txn_id in self._committed_txn_ids or entry.txn_id in self._aborted_txn_ids:
            return False

        key = (
            f"{JOURNAL_PREFIX}entry-{entry.seq:010d}-{entry.txn_id}.json"
        )
        try:
            created = self.store.put_if_absent(key, entry.to_json())
        except StoreIndeterminate:
            self.stats.indeterminate_writes += 1
            self._wait_visible(key)
            created = key in self.store.list(JOURNAL_PREFIX)

        if created:
            self._journal.append(entry)
            self._journal.sort(key=lambda e: e.seq)
            if entry.kind == "commit":
                self._committed_txn_ids.add(entry.txn_id)
            else:
                self._aborted_txn_ids.add(entry.txn_id)
        else:
            # 可能是对端并发写成功了，重建投影再继续，绝不凭内存做决定。
            self._reload_journal()

        self._flip_current()
        return created

    def _flip_current(self) -> None:
        """翻转 CURRENT 指针。崩溃也无妨：reader 能回退到全量 list 重建。"""
        payload = {
            "table": self.table,
            "last_seq": max((e.seq for e in self._journal), default=0),
            "updated_at": _utcnow(),
        }
        try:
            self.store.put(CURRENT_KEY, _jsonb(payload))
        except StoreIndeterminate:
            # CURRENT 只是缓存指针，丢了可以重建，不值得把不确定故障向上抛。
            self.stats.indeterminate_writes += 1
        except CrashInjected:
            raise

    # ---------- 可见性等待 ----------

    def _list_visible(self, prefix: str) -> list[str]:
        """list 带可见性等待：延迟可见只转化为重试，不转化为误判。

        连续两次轮询结果完全一致才认为 list 已收敛——新协调器的首轮结果
        不能直接采信，否则会在对象尚未可见时提前返回空集 / 残缺集。
        """
        prev: set[str] | None = None
        for _ in range(self.visibility_attempts):
            cur = set(self.store.list(prefix))
            if prev is not None and cur == prev:
                return sorted(cur)
            prev = cur
            self.store.tick()
        return sorted(prev or set())

    def _get_with_visibility(self, key: str) -> bytes:
        for _ in range(self.visibility_attempts):
            try:
                return self.store.get(key)
            except KeyError:
                self.stats.visibility_retries += 1
                self.store.tick()
        raise KeyError(f"{key} not visible after {self.visibility_attempts} ticks")

    def _wait_visible(self, key: str, prefix: str = JOURNAL_PREFIX) -> None:
        for _ in range(self.visibility_attempts):
            if key in self.store.list(prefix):
                return
            self.store.tick()
            self.stats.visibility_retries += 1

    # ---------- 批次号水位 ----------

    def _max_known_batch_id(self) -> int:
        bids = [e.batch_id for e in self._journal]
        for handle in self._txns.values():
            bids.append(handle.batch_id)

        # 标记文件里的 batch_id（含延迟可见、本进程尚未登记的事务目录）。
        for key in self.store.list(TXN_PREFIX) + self.store.list(QUARANTINE_PREFIX):
            if key.endswith(
                (MARKER_PREPARED, MARKER_ABORTED, MARKER_COMMITTED, "quarantine.json")
            ):
                try:
                    payload = json.loads(self.store.get(key))
                    bids.append(int(payload["batch_id"]))
                except (KeyError, ValueError):
                    continue

        # 事务目录 / 隔离目录名本身就编码了 batch_id（txn-000123-xxxxxxxx）。
        for txn_id in set(self._txn_dirs_visible()) | set(self._quarantine_ids()):
            try:
                bids.append(int(txn_id.split("-")[1]))
            except (IndexError, ValueError):
                continue

        return max(bids, default=-1)

    # ---------- 事务生命周期 ----------

    def begin_txn(self, batch_id: int | None = None) -> TxnHandle:
        if batch_id is None:
            batch_id = self._max_known_batch_id() + 1
        else:
            known = self._max_known_batch_id()
            if batch_id <= known:
                raise ValueError(
                    f"batch_id {batch_id} already allocated (high watermark={known})"
                )

        txn_id = f"txn-{batch_id:06d}-{uuid.uuid4().hex[:8]}"
        handle = TxnHandle(
            txn_id=txn_id,
            batch_id=batch_id,
            data_key=f"{TXN_PREFIX}{txn_id}/data.jsonl",
        )
        self._txns[txn_id] = handle
        self.stats.txns_begun += 1
        return handle

    def write_data(self, txn: TxnHandle, raw: bytes) -> None:
        """暂存数据。不确定故障通过"对象是否已存在"消解，绝不重复写内容。"""
        try:
            self.store.put(txn.data_key, raw)
        except StoreIndeterminate:
            self.stats.indeterminate_writes += 1
            self._wait_visible(txn.data_key, TXN_PREFIX)
            # 只有确认对象不存在时才补写；条件写保证不覆盖已经落盘的对象，
            # 因此"可能已成功"的写入永远不会被第二份内容顶替。
            if txn.data_key not in self.store.list(TXN_PREFIX):
                try:
                    self.store.put_if_absent(txn.data_key, raw)
                except StoreIndeterminate:
                    # 补写结局仍不可判定：对象可能存在也可能不存在，
                    # 留给恢复对账（无标记 → PENDING → abort），此处不抛异常。
                    self.stats.indeterminate_writes += 1

        txn.row_count = raw.count(b"\n")
        txn.checksum = zlib.crc32(raw)

    def prepare(self, txn: TxnHandle, checkpoint: int | None = None) -> None:
        """预提交：标记持久化后，本事务进入"必须给出 commit/abort 结论"窗口。"""
        if txn.state != TxnState.PENDING:
            return

        marker = {
            "txn_id": txn.txn_id,
            "batch_id": txn.batch_id,
            "data_key": txn.data_key,
            "row_count": txn.row_count,
            "checksum": txn.checksum,
            "checkpoint": checkpoint,
            "prepared_at": _utcnow(),
        }
        key = f"{TXN_PREFIX}{txn.txn_id}/{MARKER_PREPARED}"
        try:
            self.store.put_if_absent(key, _jsonb(marker))
        except StoreIndeterminate:
            self.stats.indeterminate_writes += 1
            self._wait_visible(key, TXN_PREFIX)

        self._wait_visible(key)
        txn.state = TxnState.PREPARED
        self.stats.prepared += 1

    def commit_pending(self, checkpoint: int | None = None) -> list[JournalEntry]:
        """提交所有已 PREPARED 的事务，按 batch_id 顺序发布。可重复调用、幂等。"""
        pending = sorted(
            (h for h in self._txns.values() if h.state == TxnState.PREPARED),
            key=lambda h: h.batch_id,
        )
        out: list[JournalEntry] = []
        for txn in pending:
            entry = self._commit_one(txn, checkpoint=checkpoint)
            if entry is not None:
                out.append(entry)
        return out

    def _commit_one(self, txn: TxnHandle, *, checkpoint: int | None) -> JournalEntry | None:
        if txn.txn_id in self._committed_txn_ids:
            txn.state = TxnState.COMMITTED
            return None

        # 发布前确认数据块可读：延迟可见在这里被重试吸收。
        self._get_with_visibility(txn.data_key)

        entry = JournalEntry(
            seq=self._next_journal_seq(),
            kind="commit",
            txn_id=txn.txn_id,
            batch_id=txn.batch_id,
            files=[txn.data_key],
            row_count=txn.row_count,
            checksum=txn.checksum,
            checkpoint=checkpoint,
            created_at=_utcnow(),
        )
        wrote = self._append_journal(entry)

        marker_key = f"{TXN_PREFIX}{txn.txn_id}/{MARKER_COMMITTED}"
        try:
            self.store.put_if_absent(
                marker_key,
                _jsonb(
                    {
                        "txn_id": txn.txn_id,
                        "seq": entry.seq,
                        "committed_at": _utcnow(),
                    }
                ),
            )
        except StoreIndeterminate:
            self.stats.indeterminate_writes += 1
            self._wait_visible(marker_key, TXN_PREFIX)

        txn.state = TxnState.COMMITTED
        if wrote:
            self.stats.committed += 1
        return entry

    def abort(self, txn: TxnHandle, *, reason: str) -> None:
        """明确放弃一个事务：写 abort 标记 + journal 墓碑，数据留给 GC。"""
        if txn.txn_id in self._committed_txn_ids:
            txn.state = TxnState.COMMITTED
            return

        marker = {
            "txn_id": txn.txn_id,
            "batch_id": txn.batch_id,
            "reason": reason,
            "aborted_at": _utcnow(),
        }
        try:
            self.store.put_if_absent(
                f"{TXN_PREFIX}{txn.txn_id}/{MARKER_ABORTED}", _jsonb(marker)
            )
        except StoreIndeterminate:
            self.stats.indeterminate_writes += 1
            self._wait_visible(f"{TXN_PREFIX}{txn.txn_id}/{MARKER_ABORTED}", TXN_PREFIX)

        tombstone = JournalEntry(
            seq=self._next_journal_seq(),
            kind="abort",
            txn_id=txn.txn_id,
            batch_id=txn.batch_id,
            files=[txn.data_key],
            row_count=txn.row_count,
            checksum=txn.checksum,
            checkpoint=None,
            created_at=_utcnow(),
        )
        self._append_journal(tombstone)
        txn.state = TxnState.ABORTED
        self.stats.aborted += 1

    # ---------- 恢复对账 ----------

    def _txn_dirs_visible(self) -> list[str]:
        """列出当前可见的事务目录名，容忍不可见窗口与空路径段。"""
        out: set[str] = set()
        for key in self.store.list(TXN_PREFIX):
            rest = key[len(TXN_PREFIX) :]
            if "/" in rest and rest.split("/", 1)[0]:
                out.add(rest.split("/", 1)[0])
        return sorted(out)

    def recover(
        self,
        *,
        last_checkpoint_batch: int | None = None,
        resume_offset: int | None = None,
    ) -> RecoveryReport:
        """启动对账：把存储上所有事务归入 已提交 / 已中止 / 隔离 三类。

        last_checkpoint_batch: 最近一次成功 checkpoint 里的 next_batch_id
        （即该 checkpoint 已覆盖到 batch_id < 该值的全部事务）。
        """
        with self._lock:
            report = RecoveryReport(
                lock_epoch=self._lock.epoch, resume_offset=resume_offset
            )
            self._reload_journal()

            for txn_id in self._txn_dirs_visible():
                verdict = self._classify(txn_id, last_checkpoint_batch, report)
                bid = self._batch_id_of(txn_id)
                if bid is None:
                    continue
                if verdict is TxnState.COMMITTED:
                    report.committed.append(bid)
                elif verdict is TxnState.ABORTED:
                    report.aborted.append(bid)
                elif verdict is TxnState.QUARANTINED:
                    report.quarantined.append(bid)

            # CURRENT 可能滞后于 journal（翻转前崩溃），按全量投影修复。
            visible_current = self._read_current()
            expected_seq = max((e.seq for e in self._journal), default=0)
            if visible_current != expected_seq:
                self._flip_current()
                report.journal_repaired = True

            report.committed.sort()
            report.aborted.sort()
            report.quarantined.sort()
            return report

    def _read_current(self) -> int | None:
        try:
            return int(json.loads(self.store.get(CURRENT_KEY))["last_seq"])
        except KeyError:
            return None

    def _marker(self, txn_id: str, name: str) -> dict | None:
        key = f"{TXN_PREFIX}{txn_id}/{name}"
        try:
            return json.loads(self._get_with_visibility(key))
        except KeyError:
            return None

    def _batch_id_of(self, txn_id: str) -> int | None:
        for name in (MARKER_PREPARED, MARKER_ABORTED, MARKER_COMMITTED):
            payload = self._marker(txn_id, name)
            if payload and "batch_id" in payload:
                return int(payload["batch_id"])
        try:
            return int(txn_id.split("-")[1])
        except (IndexError, ValueError):
            return None

    def _classify(
        self, txn_id: str, last_checkpoint_batch: int | None, report: RecoveryReport
    ) -> TxnState:
        committed_marker = self._marker(txn_id, MARKER_COMMITTED)
        prepared_marker = self._marker(txn_id, MARKER_PREPARED)
        aborted_marker = self._marker(txn_id, MARKER_ABORTED)
        batch_id = self._batch_id_of(txn_id)

        # 1. journal 是最高事实：有 commit 条目就是已提交，标记缺了就补建。
        if txn_id in self._committed_txn_ids:
            if committed_marker is None:
                self.store.put_if_absent(
                    f"{TXN_PREFIX}{txn_id}/{MARKER_COMMITTED}",
                    _jsonb(
                        {
                            "txn_id": txn_id,
                            "rebuilt_by": "recovery",
                            "committed_at": _utcnow(),
                        }
                    ),
                )
            return TxnState.COMMITTED

        # 2. 已有 abort 结论（journal 墓碑或 abort 标记）。
        if txn_id in self._aborted_txn_ids or aborted_marker is not None:
            return TxnState.ABORTED

        # checkpoint 覆盖口径：没有 checkpoint 信息时，PREPARED 事务按"可能已
        # 覆盖"处理（宁可隔离也不贸然 abort，abort 后重放会双发）。
        covered = (
            last_checkpoint_batch is None
            or batch_id is None
            or batch_id < last_checkpoint_batch
        )

        # 3. PREPARED 且未被任何 checkpoint 覆盖 → 确定未提交，安全 abort。
        #    注意必须先判 PENDING：没有 prepare 标记的事务根本不可能进入过
        #    提交流程，与 checkpoint 是否存在无关。
        if prepared_marker is None:
            txn = self._rehydrate(txn_id, None)
            self.abort(txn, reason="crash before prepare")
            return TxnState.ABORTED

        # 4. 已 PREPARED：有 committed 标记、或被 checkpoint 覆盖 → 结局存疑，
        #    先在 settle 窗口里等 journal 的 commit 条目。
        if committed_marker is not None or covered:
            report.indeterminate_seen += 1
            if self._settle_journal(txn_id):
                self.store.put_if_absent(
                    f"{TXN_PREFIX}{txn_id}/{MARKER_COMMITTED}",
                    _jsonb(
                        {
                            "txn_id": txn_id,
                            "rebuilt_by": "recovery",
                            "committed_at": _utcnow(),
                        }
                    ),
                )
                return TxnState.COMMITTED
            # settle 窗口内仍等不到 commit 条目：无法判定，隔离，绝不 abort。
            self._quarantine(
                txn_id, reason="commit outcome undecidable after settle window"
            )
            return TxnState.QUARANTINED

        # 5. PREPARED 但不被任何 checkpoint 覆盖 → 确定未提交，安全 abort。
        txn = self._rehydrate(txn_id, prepared_marker)
        self.abort(txn, reason="prepared but not covered by any checkpoint")
        return TxnState.ABORTED

    def _settle_journal(self, txn_id: str) -> bool:
        """在可见性窗口内反复确认 journal 是否出现该事务的 commit 条目。"""
        for _ in range(self.visibility_attempts):
            self._reload_journal()
            if txn_id in self._committed_txn_ids:
                return True
            self.store.tick()
        self._reload_journal()
        return txn_id in self._committed_txn_ids

    def _rehydrate(self, txn_id: str, prepared_marker: dict | None) -> TxnHandle:
        bid = self._batch_id_of(txn_id) or 0
        handle = TxnHandle(
            txn_id=txn_id,
            batch_id=bid,
            data_key=f"{TXN_PREFIX}{txn_id}/data.jsonl",
            state=TxnState.PREPARED if prepared_marker is not None else TxnState.PENDING,
        )
        if prepared_marker is not None:
            handle.row_count = int(prepared_marker.get("row_count", 0))
            handle.checksum = int(prepared_marker.get("checksum", 0))
        self._txns[txn_id] = handle
        return handle

    def _quarantine(self, txn_id: str, *, reason: str) -> None:
        """无法判定的事务整体搬进隔离区，数据原样保留、批次号永久作废。"""
        self._lock.check_fence()

        src_keys = [
            k for k in self.store.list(f"{TXN_PREFIX}{txn_id}/")
        ]
        qdir = f"{QUARANTINE_PREFIX}{txn_id}/"
        for key in src_keys:
            payload = self.store.get(key)
            dest = qdir + key[len(f"{TXN_PREFIX}{txn_id}/") :]
            self.store.put_if_absent(dest, payload)
            self.store.delete(key)

        record = {
            "txn_id": txn_id,
            "batch_id": self._batch_id_of(txn_id),
            "reason": reason,
            "quarantined_at": _utcnow(),
            "lock_epoch": self._lock.epoch,
        }
        self.store.put_if_absent(qdir + "quarantine.json", _jsonb(record))

        bid = record["batch_id"] if record["batch_id"] is not None else self._max_known_batch_id()
        tombstone = JournalEntry(
            seq=self._next_journal_seq(),
            kind="abort",
            txn_id=f"quarantine:{txn_id}",
            batch_id=bid,
            files=[qdir],
            row_count=0,
            checksum=0,
            checkpoint=None,
            created_at=_utcnow(),
        )
        self._append_journal(tombstone)
        self.stats.quarantined += 1

    # ---------- 垃圾回收 ----------

    def reconcile(self, grace_seconds: float = 3600.0) -> dict:
        """并发安全的垃圾回收：只清理已经在 journal 里有结论的事务目录。

        隔离区永不自动删除；没有 journal 结论的目录永不删除，因此并发清理
        不可能删掉正在提交的事务。

        grace_seconds: 事务有了 journal 结论之后，至少再保留多少**墙钟秒**才
        允许物理删除。不能用对象存储的可见性时钟做宽限——可见性重试本身会
        推进那个时钟，重复恢复会把存活数据"催老"。墙钟宽限保证慢读者与停留
        在旧 CURRENT 指针上的消费方有完整的 settle 窗口（对标 Iceberg 的
        expire-snapshots 保留期）。
        """
        with self._lock:
            now_wall = datetime.now(timezone.utc)
            self._reload_journal()
            decided = self._committed_txn_ids | self._aborted_txn_ids
            deleted_dirs: list[str] = []

            for txn_id in self._txn_dirs_visible():
                if txn_id not in decided:
                    continue
                decided_at = self._decision_walltime(txn_id)
                if decided_at is None:
                    continue
                age_seconds = (now_wall - decided_at).total_seconds()
                if age_seconds < grace_seconds:
                    continue
                keys = self.store.list(f"{TXN_PREFIX}{txn_id}/")
                for key in keys:
                    self.store.delete(key)
                    self.stats.reconciled_deletes += 1
                deleted_dirs.append(txn_id)
                self._txns.pop(txn_id, None)

            return {
                "deleted_txns": deleted_dirs,
                "quarantine_kept": self._quarantine_ids(),
            }

    def _decision_walltime(self, txn_id: str) -> datetime | None:
        for name in (MARKER_COMMITTED, MARKER_ABORTED):
            payload = self._marker(txn_id, name)
            if not payload:
                continue
            ts = payload.get("committed_at") or payload.get("aborted_at")
            if not ts:
                return None
            try:
                parsed = datetime.fromisoformat(ts)
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None

    def _quarantine_ids(self) -> list[str]:
        out: set[str] = set()
        for key in self.store.list(QUARANTINE_PREFIX):
            rest = key[len(QUARANTINE_PREFIX) :]
            if "/" in rest and rest.split("/", 1)[0]:
                out.add(rest.split("/", 1)[0])
        return sorted(out)

    # ---------- GC 协作接口 ----------

    def aborted_txn_ids(self) -> set[str]:
        """journal 里已有 abort 结论（含隔离墓碑）的事务 id。"""
        return set(self._aborted_txn_ids)

    def forget_txn(self, txn_id: str) -> None:
        """物理清理后从内存句柄表移除；不触碰存储上的任何结论。"""
        self._txns.pop(txn_id, None)

    # ---------- 下游视角 ----------

    def snapshot_entries(self) -> list[JournalEntry]:
        """下游视角：只暴露 journal 中按序排列的 commit 条目。"""
        self._reload_journal()
        return [e for e in self._journal if e.kind == "commit"]

    def read_committed(self) -> list[bytes]:
        """读取所有已发布数据块。引用缺失（延迟可见）时重试而不是报错跳过。"""
        blocks: list[bytes] = []
        for entry in self.snapshot_entries():
            for key in entry.files:
                blocks.append(self._get_with_visibility(key))
        return blocks

    def visible_batch_ids(self) -> list[int]:
        return [e.batch_id for e in self.snapshot_entries()]

    def state_view(self) -> dict[str, str]:
        """观测用：存储上每个事务目录当前判定出的状态。"""
        view: dict[str, str] = {}
        for txn_id in self._txn_dirs_visible():
            if txn_id in self._committed_txn_ids:
                view[txn_id] = TxnState.COMMITTED.value
            elif txn_id in self._aborted_txn_ids:
                view[txn_id] = TxnState.ABORTED.value
            elif self._marker(txn_id, MARKER_COMMITTED) is not None:
                # 标记在、journal 没结论：正是"无法判定"的现场。
                view[txn_id] = "indeterminate"
            elif self._marker(txn_id, MARKER_PREPARED) is not None:
                view[txn_id] = TxnState.PREPARED.value
            else:
                view[txn_id] = TxnState.PENDING.value
        for q in self._quarantine_ids():
            view[q] = TxnState.QUARANTINED.value
        return view
