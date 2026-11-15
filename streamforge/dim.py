"""维表版本切换：双缓冲构建 + 原子发布 + 内容寻址。

背景
----
旧实现是全量覆盖：``rows.clear()`` 之后逐条写入。在线计算落在 clear 与
写满之间时，会读到空维度或一半新一半旧的行。这里把"构建"和"发布"拆开：

    loader() ──► 后台构建完整快照 ──► 校验 digest/行数/内存 ──► 原子换指针
                                            │
                                            └─ 失败：保留旧版本，记录原因

关键不变量
----------
1. **原子性**：事件线程只在同一时刻读一次 ``current`` 引用，随后所有查询
   都走这个本地引用；因此一条事件不可能跨两个版本取字段。
2. **内容寻址**：版本 id 为 ``dim-<seq>-<sha256(内容)[:12]>``。digest 由
   排序后的规范 JSONL 计算，id、元数据、磁盘快照三者必须互相吻合，
   版本标识不会与内容脱节。
3. **可证明**：每条输出行写入 ``dim_version`` 与 ``dim_digest``；版本快照
   落盘到 ``_dim_versions/``，可据此逐行复算关联字段。
4. **不阻塞**：构建、校验、落盘都在后台线程；发布只做一次引用赋值。
5. **失败安全**：加载异常/空快照/重复主键/内存超限都只记录失败，旧版本
   继续服务；支持显式回退到上一个已知良好版本。
6. **刷新快于加载**：单飞（single-flight）+ 合并（coalesce）。加载中收到
   的刷新请求只置一个 pending 位，不会堆积线程或队列。
7. **检查点恢复**：检查点记录 active 版本 id；重启时从版本仓库按 id 精确
   恢复并校验 digest。恢复失败则退回最新可用版本，并明确记录。
8. **内存峰值**：候选版本在构建阶段即按规范字节数计费；旧版本 + 候选版本
   超过预算时先丢弃回退用的上一版本，仍不够则拒绝发布。
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

DimLoader = Callable[[], Iterable[Mapping]]


# ---------------------------------------------------------------- 异常


class DimError(RuntimeError):
    """维表相关错误基类。"""


class DimLoadError(DimError):
    """快照加载/校验失败。旧版本保持可用。"""


class DimMemoryLimitError(DimLoadError):
    """快照超过内存预算。"""


class DimVersionVerifyError(DimError):
    """版本 id / digest / 磁盘内容不一致。"""


# ---------------------------------------------------------------- 版本


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def canonical_row(row: Mapping) -> str:
    """规范化一行 JSON：键排序、无多余空白、单行输出。"""
    return json.dumps(
        dict(row),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _digest_lines(lines: Iterable[str]) -> str:
    h = hashlib.sha256()
    for line in lines:
        h.update(line.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def content_digest(rows: Iterable[Mapping]) -> str:
    """对一批行按 shop_id 排序后计算 digest（测试/校验用）。"""
    by_id: dict[str, Mapping] = {}
    for r in rows:
        by_id[str(r["shop_id"])] = r
    return _digest_lines(canonical_row(by_id[k]) for k in sorted(by_id))


@dataclass(frozen=True)
class DimVersion:
    """一个不可变的维表快照。

    ``rows`` 的外层与每行都做了只读封装，避免运行期被意外修改导致
    “id 说的是一套、内容却是另一套”。
    """

    seq: int
    digest: str
    rows: Mapping[str, Mapping]
    row_count: int
    size_bytes: int
    loaded_at: datetime
    source: str = "loader"

    @property
    def id(self) -> str:
        return f"dim-{self.seq:06d}-{self.digest[:12]}"

    def lookup(self, shop_id: str) -> Mapping | None:
        return self.rows.get(shop_id)

    def canonical_lines(self) -> list[str]:
        return [canonical_row(self.rows[k]) for k in sorted(self.rows)]

    def verify(self) -> bool:
        if self.row_count != len(self.rows):
            return False
        if not self.id.endswith(self.digest[:12]):
            return False
        return _digest_lines(self.canonical_lines()) == self.digest


def _freeze(rows: Mapping[str, Mapping]) -> Mapping[str, Mapping]:
    """冻结外层映射与每一行，发布后内容不可变。"""
    return MappingProxyType(
        {k: v if isinstance(v, MappingProxyType) else MappingProxyType(dict(v)) for k, v in rows.items()}
    )


# ---------------------------------------------------------------- 版本仓库


class DimVersionStore:
    """把版本快照落到磁盘，供重启恢复与逐行取证。

    目录结构::

        _dim_versions/
            dim-000001-abcdef123456.json       # 元数据（提交标记）
            dim-000001-abcdef123456.jsonl      # 规范 JSONL，按 shop_id 排序

    先写 rows 再写 meta；meta 用 os.replace 原子落位，meta 存在即视为
    该版本完整。读取时会重新计算 digest，并校验 id 中的 seq/digest 前缀。
    """

    FORMAT = 1

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        # 上次进程被杀留下的临时文件：meta 从未落位，不可能被引用。
        for stale in self.root.glob(".tmp-*"):
            stale.unlink(missing_ok=True)

    def _paths(self, version_id: str) -> tuple[Path, Path]:
        return self.root / f"{version_id}.jsonl", self.root / f"{version_id}.json"

    def put(self, version: DimVersion, *, verify: bool = True) -> None:
        rows_path, meta_path = self._paths(version.id)
        if meta_path.exists():
            # 同一 id 意味着同一 seq + 同一 digest；已存在且完整则幂等返回。
            # 若旧文件损坏（例如上次写到一半），则继续往下重写修复。
            try:
                existing = self.get(version.id)
            except DimVersionVerifyError:
                existing = None
            if existing is not None:
                if existing.digest != version.digest:
                    raise DimVersionVerifyError(f"version id collision: {version.id}")
                return

        tmp_rows = rows_path.with_name(f".tmp-{os.getpid()}-{threading.get_ident()}.jsonl")
        tmp_meta = meta_path.with_name(f".tmp-{os.getpid()}-{threading.get_ident()}.json")
        try:
            with tmp_rows.open("w", encoding="utf-8") as fh:
                for line in version.canonical_lines():
                    fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            meta = {
                "format": self.FORMAT,
                "version": version.id,
                "seq": version.seq,
                "digest": version.digest,
                "row_count": version.row_count,
                "size_bytes": version.size_bytes,
                "loaded_at": version.loaded_at.isoformat(),
                "source": version.source,
            }
            tmp_meta.write_text(
                json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            os.replace(tmp_rows, rows_path)
            os.replace(tmp_meta, meta_path)  # meta 最后落位 = 提交
        finally:
            tmp_rows.unlink(missing_ok=True)
            tmp_meta.unlink(missing_ok=True)

        if verify:
            self.get(version.id)

    def get(self, version_id: str) -> DimVersion:
        rows_path, meta_path = self._paths(version_id)
        if not meta_path.exists() or not rows_path.exists():
            raise DimVersionVerifyError(f"version snapshot missing: {version_id}")
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DimVersionVerifyError(f"corrupt version meta {version_id}: {exc}") from exc

        if meta.get("version") != version_id:
            raise DimVersionVerifyError(f"meta version mismatch: {version_id}")
        seq = int(meta.get("seq", -1))
        expected_prefix = f"dim-{seq:06d}-"
        if not version_id.startswith(expected_prefix):
            raise DimVersionVerifyError(f"version id seq mismatch: {version_id}")

        rows: dict[str, Mapping] = {}
        lines: list[str] = []
        try:
            with rows_path.open("r", encoding="utf-8") as fh:
                for raw in fh:
                    if not raw.strip():
                        continue
                    row = json.loads(raw)
                    sid = row.get("shop_id")
                    if not isinstance(sid, str) or not sid:
                        raise DimVersionVerifyError(f"bad shop_id in {version_id}")
                    if sid in rows:
                        raise DimVersionVerifyError(f"duplicate shop_id in {version_id}")
                    rows[sid] = row
                    lines.append(canonical_row(row))
        except (OSError, json.JSONDecodeError) as exc:
            raise DimVersionVerifyError(f"corrupt version rows {version_id}: {exc}") from exc

        digest = _digest_lines(lines)
        if digest != meta.get("digest"):
            raise DimVersionVerifyError(f"digest mismatch for {version_id}")
        if not version_id.endswith(str(digest)[:12]):
            raise DimVersionVerifyError(f"version id digest mismatch: {version_id}")
        if len(rows) != int(meta.get("row_count", -1)):
            raise DimVersionVerifyError(f"row_count mismatch for {version_id}")

        try:
            loaded_at = datetime.fromisoformat(meta["loaded_at"])
        except (KeyError, ValueError):
            loaded_at = _utcnow()
        return DimVersion(
            seq=seq,
            digest=digest,
            rows=_freeze(rows),
            row_count=len(rows),
            size_bytes=int(meta.get("size_bytes", 0)),
            loaded_at=loaded_at,
            source=str(meta.get("source", "store")),
        )

    def latest(self) -> DimVersion | None:
        metas = sorted(self.root.glob("dim-*.json"))
        for path in reversed(metas):
            try:
                return self.get(path.stem)
            except DimVersionVerifyError:
                # 损坏的版本不参与回退；继续找更旧的。
                continue
        return None

    def find_by_digest(self, digest: str) -> DimVersion | None:
        """按内容 digest 找已落盘的版本（重启后复用同一版本 id）。"""
        for path in sorted(self.root.glob(f"dim-*-{digest[:12]}.json"), reverse=True):
            try:
                version = self.get(path.stem)
            except DimVersionVerifyError:
                continue
            if version.digest == digest:
                return version
        return None

    def ids(self) -> list[str]:
        return [p.stem for p in sorted(self.root.glob("dim-*.json"))]

    def next_seq(self) -> int:
        """磁盘上已用过的最大 seq + 1（不与历史版本撞号）。

        只读元数据里的 seq，不重新校验内容；损坏版本占用的编号也不会被复用。
        """
        best = 0
        for path in self.root.glob("dim-*.json"):
            try:
                seq = int(json.loads(path.read_text(encoding="utf-8")).get("seq", 0))
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            best = max(best, seq)
        return best + 1


def row_provenance_ok(
    row: Mapping, store: DimVersionStore | None = None, *, version: DimVersion | None = None
) -> tuple[bool, str]:
    """证明一行使用的是它声明的维度版本。

    检查三件事：版本快照存在且 digest 自洽、行上的 digest 与版本一致、
    行的关联字段与该版本中 shop_id 对应的内容完全一致。

    传入 ``version`` 可避免批量校验时反复读盘（调用方负责按 id 缓存）。
    """
    version_id = row.get("dim_version")
    if not version_id:
        return False, "missing dim_version"
    if version is None:
        if store is None:
            return False, "no version store"
        try:
            version = store.get(str(version_id))
        except DimVersionVerifyError as exc:
            return False, str(exc)
    elif version.id != version_id:
        return False, "resolved version id mismatch"
    if row.get("dim_digest") != version.digest:
        return False, "row dim_digest != version digest"
    dim = version.lookup(str(row.get("shop_id")))
    if dim is None:
        ok = row.get("shop_name") is None and row.get("region") is None
        return (ok, "missing shop should yield null dims" if not ok else "ok")
    if row.get("shop_name") != dim.get("shop_name") or row.get("region") != dim.get("region"):
        return False, "enriched fields do not match declared version"
    return True, "ok"


# ---------------------------------------------------------------- 统计


@dataclass
class DimStats:
    activations: int = 0          # 任何一次成功切换（加载/恢复/回退）
    loads: int = 0                # 成功发布的新加载
    failures: int = 0             # 加载失败次数
    requests: int = 0             # 收到的刷新请求
    coalesced: int = 0            # 因在途加载而被合并的请求
    rollbacks: int = 0
    restores: int = 0
    restore_failures: int = 0
    memory_rejections: int = 0
    consistency_failures: int = 0
    lookups: int = 0
    lookups_missed: int = 0
    # 兼容旧快照字段：原子切换后 reload 期间不会再有空窗，恒为 0。
    miss_during_reload: int = 0
    peak_bytes: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None


# ---------------------------------------------------------------- 版本表


class DimTable:
    """带原子版本切换与后台刷新的维表。

    事件线程只需要 ``current`` 一次：

        version = table.current          # 原子引用读取
        dim = version.lookup(shop_id)    # 整条事件都走同一版本

    刷新线程通过 ``request_refresh()`` 触发；同一条加载链路上永远不会出现
    两个并发的构建任务。
    """

    def __init__(
        self,
        loader: DimLoader,
        *,
        store: DimVersionStore | None = None,
        max_bytes: int = 512 * 1024 * 1024,
        allow_empty: bool = False,
        allow_duplicate_keys: bool = False,
        retain_versions: int = 2,
        verify_on_swap: bool = True,
        background: bool = True,
    ) -> None:
        self.loader = loader
        self.store = store
        self.max_bytes = max(1, int(max_bytes))
        self.allow_empty = allow_empty
        self.allow_duplicate_keys = allow_duplicate_keys
        # 目前只区分"只留活动版本"与"额外留一个可回退版本"。
        self.retain_versions = 1 if int(retain_versions) < 2 else 2
        self.verify_on_swap = verify_on_swap

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._load_mutex = threading.Lock()  # 串行化构建/恢复/回退

        self._current: DimVersion | None = None
        self._previous: DimVersion | None = None
        self._versions: dict[str, DimVersion] = {}
        self._switch_log: list[dict] = []
        self._seq_next = store.next_seq() if store is not None else 1
        self.stats = DimStats()

        self._pending = False
        self._pending_loader: DimLoader | None = None
        self._inflight = False
        self._closed = False
        self._worker: threading.Thread | None = None
        # 后台线程按需启动（第一次 request_refresh），避免只读表也常驻线程。
        self._background = background

    # ---------- 读取路径（不阻塞、不跨版本） ----------

    @property
    def current(self) -> DimVersion | None:
        # CPython 中属性读取是原子的；构造最后一个版本对象时已完整冻结。
        return self._current

    @property
    def active_id(self) -> str | None:
        v = self._current
        return v.id if v else None

    @property
    def active_digest(self) -> str | None:
        v = self._current
        return v.digest if v else None

    def lookup(self, shop_id: str) -> Mapping | None:
        v = self._current
        if v is None:
            self.note_lookup(missed=True)
            return None
        found = v.lookup(shop_id)
        self.note_lookup(missed=found is None)
        return found

    def note_lookup(self, *, missed: bool) -> None:
        # 热路径不取锁：发布只做引用赋值，查询与发布之间无需互斥。
        # 计数仅用于观测，极端并发下允许少量丢失。
        self.stats.lookups += 1
        if missed:
            self.stats.lookups_missed += 1

    # ---------- 加载/发布 ----------

    def _build(self, supplier: DimLoader, budget: int, seq: int) -> DimVersion:
        rows: dict[str, Mapping] = {}
        # 每行的规范化字节数单独记，重复主键覆盖时可回滚计数，
        # size_bytes 因而与最终快照严格一致。
        size_by_sid: dict[str, int] = {}
        size = 0
        duplicates = 0
        try:
            iterator = iter(supplier())
        except Exception as exc:  # noqa: BLE001 - 加载源可能有任意异常
            raise DimLoadError(f"dim loader failed: {exc}") from exc

        try:
            for r in iterator:
                if not isinstance(r, Mapping):
                    raise DimLoadError(f"dim row is not a mapping: {type(r).__name__}")
                sid = r.get("shop_id")
                if not isinstance(sid, str) or not sid:
                    raise DimLoadError("dim row missing non-empty shop_id")
                row = dict(r)
                if sid in size_by_sid:
                    duplicates += 1
                    size -= size_by_sid[sid]
                n = len(canonical_row(row).encode("utf-8")) + 1
                rows[sid] = row
                size_by_sid[sid] = n
                size += n
                # 早停：候选版本超出预算时不再继续拉取，控制内存峰值。
                if size > budget:
                    raise DimMemoryLimitError(
                        f"dim snapshot exceeds memory budget ({size} > {budget} bytes)"
                    )
        except (DimLoadError, DimMemoryLimitError):
            raise
        except Exception as exc:  # noqa: BLE001 - 迭代过程中抛出的源错误
            raise DimLoadError(f"dim loader failed: {exc}") from exc

        if not rows and not self.allow_empty:
            raise DimLoadError("dim snapshot is empty")
        if duplicates and not self.allow_duplicate_keys:
            raise DimLoadError(f"dim snapshot has {duplicates} duplicate shop_id")

        digest = _digest_lines(canonical_row(rows[k]) for k in sorted(rows))
        return DimVersion(
            seq=seq,
            digest=digest,
            rows=_freeze(rows),
            row_count=len(rows),
            size_bytes=size,
            loaded_at=_utcnow(),
            source="loader",
        )

    def _publish_locked(self, version: DimVersion, reason: str) -> None:
        old = self._current
        previous = self._previous
        if previous is not None and (old is None or previous.id == old.id):
            previous = None

        # 发布瞬间三者可能同时在内存里：候选 + 旧版本 + 已保留的上一版本。
        def peak_bytes(prev: DimVersion | None) -> int:
            return (
                version.size_bytes
                + (old.size_bytes if old is not None else 0)
                + (prev.size_bytes if prev is not None else 0)
            )

        # 超预算时先牺牲回退能力（丢弃上一版本），仍超则拒绝切换。
        if peak_bytes(previous) > self.max_bytes:
            previous = None
        if peak_bytes(previous) > self.max_bytes:
            raise DimMemoryLimitError("candidate + active exceed memory budget")
        self.stats.peak_bytes = max(self.stats.peak_bytes, peak_bytes(previous))

        self._current = version
        self._previous = old if (old is not None and self.retain_versions >= 2) else None

        # 只保留 current/previous 上限内的版本。
        retained = [v for v in (self._current, self._previous) if v is not None]
        self._versions = {v.id: v for v in retained}

        live = sum(v.size_bytes for v in self._versions.values())
        self.stats.peak_bytes = max(self.stats.peak_bytes, live)
        self.stats.activations += 1
        self._switch_log.append(
            {
                "at": _utcnow().isoformat(),
                "from": old.id if old else None,
                "to": version.id,
                "reason": reason,
                "rows": version.row_count,
                "digest": version.digest,
            }
        )
        self._switch_log = self._switch_log[-32:]

    def _load_once(self, supplier: DimLoader, reason: str = "load") -> DimVersion:
        with self._load_mutex:
            with self._lock:
                current = self._current
                previous = self._previous
            reserved = (current.size_bytes if current else 0) + (
                previous.size_bytes if previous else 0
            )
            # 候选版本构建时就计入窗口内仍保留的版本，保证峰值不超预算。
            budget = self.max_bytes - reserved
            if budget <= 0:
                raise DimMemoryLimitError("no memory budget left for a new snapshot")
            with self._lock:
                seq = self._seq_next
            if self.store is not None:
                seq = max(seq, self.store.next_seq())
            with self._lock:
                # 先占号再构建：构建失败会留下编号空洞，但绝不产生重复 id。
                self._seq_next = seq + 1
            version = self._build(supplier, budget, seq)

            if self.store is not None:
                # 内容未变：复用已落盘的同内容版本，避免重启后重复编号。
                same = self.store.find_by_digest(version.digest)
                if same is not None:
                    version = same
                    with self._lock:
                        self._seq_next = max(self._seq_next, same.seq + 1)
                else:
                    # 先落盘并回读校验，再换指针：写进结果行的 id 一定可恢复。
                    self.store.put(version, verify=self.verify_on_swap)
            if self.verify_on_swap and not version.verify():
                raise DimVersionVerifyError(f"candidate version failed self-check: {version.id}")

            with self._lock:
                if current is not None and current.id == version.id:
                    # 同一份内容已在服务；不制造无意义的切换事件。
                    return version
                # 构建期间的峰值就是"候选 + 仍在保留的版本"，此时还未淘汰任何东西。
                self.stats.peak_bytes = max(self.stats.peak_bytes, reserved + version.size_bytes)
                self._publish_locked(version, reason)
                self.stats.loads += 1
            return version

    def load_blocking(self, supplier: DimLoader | None = None, *, reason: str = "load") -> DimVersion:
        """同步加载并发布。仅供启动/测试使用；事件线程不走这里。"""
        supplier = supplier or self.loader
        with self._lock:
            self.stats.requests += 1
        try:
            return self._load_once(supplier, reason=reason)
        except Exception as exc:
            self._record_failure(exc)
            raise

    def reload(self, rows: Iterable[Mapping] | None = None) -> DimVersion:
        """兼容旧接口：用给定行同步发布一个新版本。"""
        supplier: DimLoader = (lambda: rows) if rows is not None else self.loader
        return self.load_blocking(supplier)

    # ---------- 后台刷新 ----------

    def start_background(self) -> None:
        with self._lock:
            if not self._background:
                raise DimError("background refresh disabled for this table")
            if self._worker is not None and self._worker.is_alive():
                return
            if self._closed:
                raise DimError("dim table is closed")
            self._worker = threading.Thread(
                target=self._worker_loop, name="dim-refresh", daemon=True
            )
            self._worker.start()

    def request_refresh(self, loader: DimLoader | None = None) -> bool:
        """非阻塞地请求一次刷新。

        返回 True 表示本次请求会启动一次新的加载；False 表示已被合并到
        在途加载之后（仍然保证之后会再加载一次）。
        """
        with self._cond:
            if self._closed:
                return False
            if loader is not None:
                self._pending_loader = loader
            self.stats.requests += 1
            if self._inflight or self._pending:
                self._pending = True
                self.stats.coalesced += 1
                self._cond.notify_all()
                return False
            self._pending = True
            supplier = self._pending_loader or self.loader
            self._cond.notify_all()
        if not self._background:
            # 没有后台线程时退化为同步加载；调用方本就选择了阻塞模式。
            try:
                self._load_once(supplier, reason="sync-refresh")
            except Exception as exc:  # noqa: BLE001
                self._record_failure(exc)
                raise
            finally:
                with self._cond:
                    self._pending = False
                    self._pending_loader = None
                    self._cond.notify_all()
            return True
        self._ensure_worker()
        return True

    def _ensure_worker(self) -> None:
        if not self._background:
            return
        with self._lock:
            alive = self._worker is not None and self._worker.is_alive()
            closed = self._closed
            if closed:
                # close() 与 request_refresh() 竞争：丢弃这次请求，避免挂起等待。
                self._pending = False
                self._pending_loader = None
                self._cond.notify_all()
        if not alive and not closed:
            self.start_background()

    def _worker_loop(self) -> None:
        while True:
            with self._cond:
                while not self._pending and not self._closed:
                    self._cond.wait()
                if self._closed and not self._pending:
                    return
                supplier = self._pending_loader or self.loader
                self._pending_loader = None
                self._pending = False
                self._inflight = True
            try:
                self._load_once(supplier, reason="background-refresh")
            except Exception as exc:  # noqa: BLE001 - 后台线程不能死
                self._record_failure(exc)
            finally:
                with self._cond:
                    self._inflight = False
                    self._cond.notify_all()

    def wait_idle(self, timeout: float | None = 5.0) -> bool:
        """等待所有已请求的刷新完成。仅测试/关停路径使用。"""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while (self._pending or self._inflight) and not self._closed:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return not self._inflight

    def close(self, timeout: float | None = 5.0) -> None:
        """停止后台线程；在途加载完成后退出，未开始的请求被丢弃。"""
        with self._cond:
            self._closed = True
            self._pending = False
            self._pending_loader = None
            self._cond.notify_all()
            worker = self._worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout)

    # ---------- 恢复 / 回退 / 一致性 ----------

    def restore(self, version_id: str, *, reason: str = "restore") -> DimVersion:
        """按 id 从版本仓库恢复；digest 必须与磁盘内容吻合。"""
        if self.store is None:
            raise DimError("no version store configured")
        with self._load_mutex:
            try:
                version = self.store.get(version_id)
            except DimVersionVerifyError:
                with self._lock:
                    self.stats.restore_failures += 1
                raise
            try:
                with self._lock:
                    current = self._current
                    if current is not None and current.id == version.id:
                        # 启动时已由同一份内容发布；恢复是幂等的，不产生切换。
                        self.stats.restores += 1
                        return current
                    self._publish_locked(version, reason)
                    self.stats.restores += 1
                    self._seq_next = max(self._seq_next, version.seq + 1)
            except DimMemoryLimitError:
                with self._lock:
                    self.stats.restore_failures += 1
                raise
            return version

    def rollback(self, version_id: str | None = None) -> DimVersion:
        """原子回退到指定版本（默认上一版本）。

        回退复用原版本对象与其 id，因此已有行的版本标识依然有效。
        """
        with self._load_mutex:
            with self._lock:
                target_id = version_id
                if target_id is None:
                    target = self._previous
                    if target is None:
                        raise DimError("no previous version to roll back to")
                else:
                    target = self._versions.get(target_id)
            if target is None:
                if self.store is None:
                    raise DimError(f"version not retained and no store: {target_id}")
                target = self.store.get(target_id)
            with self._lock:
                if self._current is not None and target.id == self._current.id:
                    return target
                self._publish_locked(target, "rollback")
                self.stats.rollbacks += 1
            return target

    def assert_consistent(self) -> dict:
        """检查 active 版本与磁盘快照一致；不一致则抛出。"""
        with self._lock:
            version = self._current
        if version is None:
            raise DimVersionVerifyError("no active dim version")
        if not version.verify():
            with self._lock:
                self.stats.consistency_failures += 1
            raise DimVersionVerifyError(f"active version content mismatch: {version.id}")
        if self.store is not None:
            try:
                persisted = self.store.get(version.id)
            except DimVersionVerifyError:
                with self._lock:
                    self.stats.consistency_failures += 1
                raise
            if persisted.digest != version.digest:
                with self._lock:
                    self.stats.consistency_failures += 1
                raise DimVersionVerifyError(f"persisted digest mismatch: {version.id}")
        return {"version": version.id, "digest": version.digest, "rows": version.row_count}

    # ---------- 观测 ----------

    def _record_failure(self, exc: BaseException) -> None:
        with self._lock:
            self.stats.failures += 1
            if isinstance(exc, DimMemoryLimitError):
                self.stats.memory_rejections += 1
            self.stats.last_error = f"{type(exc).__name__}: {exc}"
            self.stats.last_error_at = _utcnow()
        with self._cond:
            self._cond.notify_all()

    def is_healthy(self) -> bool:
        """活动版本存在且自洽；失败计数单独暴露在 report 里，不污染健康判定。"""
        version = self._current
        return version is not None and version.verify()

    def report(self) -> dict:
        with self._lock:
            current = self._current
            previous = self._previous
            return {
                # 兼容旧字段：成功激活次数 / 空窗 miss（恒为 0）
                "reloads": self.stats.activations,
                "miss_during_reload": self.stats.miss_during_reload,
                # 版本标识
                "active_version": current.id if current else None,
                "active_digest": current.digest if current else None,
                "active_rows": current.row_count if current else 0,
                "previous_version": previous.id if previous else None,
                # 加载与切换
                "loads": self.stats.loads,
                "load_failures": self.stats.failures,
                "refresh_requests": self.stats.requests,
                "coalesced_requests": self.stats.coalesced,
                "rollbacks": self.stats.rollbacks,
                "restores": self.stats.restores,
                "restore_failures": self.stats.restore_failures,
                "memory_rejections": self.stats.memory_rejections,
                "consistency_failures": self.stats.consistency_failures,
                # 查询
                "lookups": self.stats.lookups,
                "lookups_missed": self.stats.lookups_missed,
                "healthy": current is not None and current.verify(),
                # 内存
                "peak_bytes": self.stats.peak_bytes,
                "max_bytes": self.max_bytes,
                "retained_versions": len(self._versions),
                "retained": [
                    {"version": v.id, "rows": v.row_count, "bytes": v.size_bytes}
                    for v in sorted(self._versions.values(), key=lambda v: v.seq)
                ],
                "last_error": self.stats.last_error,
                "last_error_at": self.stats.last_error_at.isoformat()
                if self.stats.last_error_at
                else None,
                "switches": list(self._switch_log),
            }
