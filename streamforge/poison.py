"""毒消息隔离与死信队列（DLQ）。

生产事件流里混着三类坏数据：

    截断 / 非法 JSON      连反序列化都过不去
    未知 schema 版本      版本号不在作业支持范围内
    字段类型漂移          反序列化能过，但基本契约（必填字段、类型）不满足

旧实现里这些消息要么让整个作业抛异常失败（一条坏消息卡住全部分区），
要么被 ``getOrElse`` 成默认值后悄悄混进正常窗口，事后无法判断丢了什么。

本模块给出明确语义：

1. 坏消息**绝不进入**正常窗口，统一转成 :class:`DeadLetter` 落 DLQ；
2. 每条死信保留足以复现问题的现场：原始负载（截断后的原文也留）、
   原始标识（topic/partition/offset）、失败类别、schema 版本、处理时间、
   重试次数与错误摘要；
3. 毒性消息被**毒丸台账（poison ledger）**记录指纹，重启后再次遇到会被
   直接跳过，不会无限触发失败（崩溃→重放→崩溃的毒丸循环）；
4. DLQ 下游不可用时写本地溢出文件并在恢复后补投，坏消息不会因为
   DLQ 故障而丢失，也不会倒灌回主流；
5. 死信按批落盘，单批失败不影响其它批次与正常输入。

与 Flink 的对应关系：DLQ 是一个专用 side output（``OutputTag[DeadLetter]``），
毒丸台账对应一份持久化的 ``MapState<fingerprint, PoisonEntry>``，
本地溢出对应 Kafka producer 缓冲区不可用时的本地 fallback 文件。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol


class FailureCategory(str, Enum):
    """失败类别。取值稳定，供下游按类别统计与告警。"""

    DESERIALIZATION = "deserialization_error"
    UNKNOWN_VERSION = "unknown_version"
    CONTRACT_VIOLATION = "contract_violation"
    DLQ_DOWNSTREAM = "dlq_downstream_error"


# ---------------------------------------------------------------- 死信记录


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DeadLetter:
    """一条死信。字段全部可 JSON 序列化，便于直接落 JSONL / 对象存储。"""

    # 复现问题的最小现场
    raw_payload: str
    failure_category: str
    error_summary: str
    schema_version: str | None
    # 原始标识：Kafka 三元组唯一定位一条消息，重放时可凭它回查上游
    topic: str
    partition: int
    offset: int
    # 处理时间（死信产生时刻），用于界定影响范围与 SLA
    failed_at: str
    # 第几次处理这条消息（首投为 1，重投递递增）
    attempt: int = 1
    # 同一条原始消息的稳定指纹，用于去重与毒丸台账
    fingerprint: str = ""
    # 能从坏负载里抢救出来的标识（可能为 None）
    event_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def message_fingerprint(topic: str, partition: int, offset: int, raw: str) -> str:
    """原始消息的稳定指纹。

    优先用 Kafka 三元组（同一分区内 offset 唯一且不变）；
    三元组缺失（如文件回放）时退化为负载哈希。
    指纹在重启前后保持稳定，是毒丸去重的基础。
    """
    h = hashlib.sha256()
    h.update(topic.encode("utf-8"))
    h.update(b"\x1f")
    h.update(str(partition).encode("ascii"))
    h.update(b"\x1f")
    h.update(str(offset).encode("ascii"))
    h.update(b"\x1f")
    h.update(raw.encode("utf-8", errors="replace"))
    return h.hexdigest()


# ---------------------------------------------------------------- 输入信封


@dataclass
class RawMessage:
    """进入作业的原始消息信封，携带 Kafka 定位信息。"""

    raw: str
    topic: str = "order_events"
    partition: int = 0
    offset: int = 0
    # 上游重投递时给出的投递序号；None 视为首次投递
    delivery_attempt: int = 1


@dataclass
class DecodedMessage:
    """通过校验的消息：版本化负载 + 原始定位信息。"""

    payload: dict[str, Any]
    version: str
    topic: str
    partition: int
    offset: int
    delivery_attempt: int
    fingerprint: str

    @property
    def event_id(self) -> str | None:
        eid = self.payload.get("event_id")
        return eid if isinstance(eid, str) and eid else None


# ---------------------------------------------------------------- 版本化解码与契约校验


SUPPORTED_VERSIONS = ("1",)
DEFAULT_VERSION = "1"

# (字段名, 允许的类型)。bool 是 int 的子类，金额字段显式排除 bool。
_CONTRACT: dict[str, tuple[type, ...]] = {
    "event_id": (str,),
    "order_id": (str,),
    "shop_id": (str,),
    "status": (str,),
    "amount_cents": (int,),
    "channel": (str,),
    "event_time": (str,),
    "ingest_time": (str,),
}


class DecodeError(Exception):
    """解码 / 契约校验失败，携带失败类别与可抢救的版本、事件标识。"""

    def __init__(
        self,
        category: FailureCategory,
        message: str,
        *,
        version: str | None = None,
        event_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.version = version
        self.event_id = event_id


def _extract_version(obj: Any) -> str:
    raw_version = obj.get("schema_version", DEFAULT_VERSION)
    if isinstance(raw_version, bool) or not isinstance(raw_version, (str, int)):
        raise DecodeError(
            FailureCategory.CONTRACT_VIOLATION,
            f"schema_version 类型非法: {type(raw_version).__name__}",
        )
    return str(raw_version)


def _check_contract(obj: dict[str, Any], version: str) -> None:
    for field_name, allowed in _CONTRACT.items():
        if field_name not in obj:
            raise DecodeError(
                FailureCategory.CONTRACT_VIOLATION,
                f"缺少必填字段: {field_name}",
                version=version,
                event_id=obj.get("event_id") if isinstance(obj.get("event_id"), str) else None,
            )
        value = obj[field_name]
        if isinstance(value, bool) or not isinstance(value, allowed):
            raise DecodeError(
                FailureCategory.CONTRACT_VIOLATION,
                f"字段 {field_name} 类型漂移: 期望 {_type_names(allowed)}, "
                f"实际 {type(value).__name__}",
                version=version,
                event_id=obj.get("event_id") if isinstance(obj.get("event_id"), str) else None,
            )
    # 时间字段除了是字符串，还必须是合法 ISO 8601，否则窗口归属无法计算。
    for ts_field in ("event_time", "ingest_time"):
        try:
            datetime.fromisoformat(obj[ts_field])
        except (ValueError, TypeError):
            raise DecodeError(
                FailureCategory.CONTRACT_VIOLATION,
                f"字段 {ts_field} 不是合法 ISO 8601 时间: {obj[ts_field]!r}",
                version=version,
                event_id=obj.get("event_id") if isinstance(obj.get("event_id"), str) else None,
            )


def _type_names(types: tuple[type, ...]) -> str:
    return "/".join(t.__name__ for t in types)


def decode_message(msg: RawMessage) -> DecodedMessage:
    """把原始消息解码成版本化、过契约的 :class:`DecodedMessage`。

    失败时抛 :class:`DecodeError`，调用方负责转死信，绝不让异常冒泡到主循环。
    三级防线：

    1. JSON 反序列化（截断、非法转义在此失败）
    2. 版本识别（未知版本在此失败，作业只承诺支持 ``SUPPORTED_VERSIONS``）
    3. 基本契约（缺字段、类型漂移、非法时间在此失败）
    """
    raw = msg.raw
    try:
        obj = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        raise DecodeError(
            FailureCategory.DESERIALIZATION,
            f"JSON 反序列化失败: {exc}",
        ) from exc

    if not isinstance(obj, dict):
        raise DecodeError(
            FailureCategory.DESERIALIZATION,
            f"顶层结构不是 JSON 对象: {type(obj).__name__}",
        )

    version = _extract_version(obj)
    if version not in SUPPORTED_VERSIONS:
        raise DecodeError(
            FailureCategory.UNKNOWN_VERSION,
            f"未知 schema 版本: {version}，作业支持 {list(SUPPORTED_VERSIONS)}",
            version=version,
            event_id=obj.get("event_id") if isinstance(obj.get("event_id"), str) else None,
        )

    _check_contract(obj, version)

    return DecodedMessage(
        payload=obj,
        version=version,
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        delivery_attempt=msg.delivery_attempt,
        fingerprint=message_fingerprint(msg.topic, msg.partition, msg.offset, raw),
    )


# ---------------------------------------------------------------- 毒丸台账


@dataclass
class PoisonEntry:
    """毒丸台账中的一条记录。"""

    fingerprint: str
    topic: str
    partition: int
    offset: int
    category: str
    first_seen_at: str
    occurrences: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class PoisonLedger:
    """持久化毒丸台账：重启后仍认得同一条毒性消息。

    台账是"已判定为毒丸"的事实来源。消息首次进 DLQ 时登记指纹；
    作业重启、Kafka 重投递把同一 offset 的消息再送进来时，
    :meth:`is_poison` 命中即跳过——既不会再次抛错，也不会重复写 DLQ。

    存储为 JSONL（每行一条），追加写 + 启动时回放，模拟 Flink 里
    随 checkpoint 持久化的 MapState。

    台账采用 append-only 事件溯源：每次登记（含重复登记）都追加一条完整
    快照，重启时按 fingerprint 取最后一条。崩溃在半行只会损失最后一条，
    不会破坏整个台账；occurrences 也因此能跨重启恢复。
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, PoisonEntry] = {}
        self._reload()

    def _reload(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    # 台账自身损坏不应拖垮作业：跳过坏行，隔离问题。
                    continue
                entry = PoisonEntry(
                    fingerprint=d["fingerprint"],
                    topic=d.get("topic", ""),
                    partition=d.get("partition", -1),
                    offset=d.get("offset", -1),
                    category=d.get("category", FailureCategory.DESERIALIZATION.value),
                    first_seen_at=d.get("first_seen_at", ""),
                    occurrences=d.get("occurrences", 1),
                )
                # 后写的快照覆盖先写的，最终态即该 fingerprint 的最后一条。
                self._entries[entry.fingerprint] = entry

    def is_poison(self, fingerprint: str) -> bool:
        return fingerprint in self._entries

    def record(self, letter: DeadLetter) -> bool:
        """登记一条毒丸。

        返回 True 表示首次登记；False 表示指纹已存在（重投递），
        只累加出现次数。两种情况都追加落盘，保证计数重启后可恢复。
        """
        existing = self._entries.get(letter.fingerprint)
        if existing is not None:
            existing.occurrences += 1
            self._append(existing)
            return False
        entry = PoisonEntry(
            fingerprint=letter.fingerprint,
            topic=letter.topic,
            partition=letter.partition,
            offset=letter.offset,
            category=letter.failure_category,
            first_seen_at=letter.failed_at,
        )
        self._entries[entry.fingerprint] = entry
        self._append(entry)
        return True

    def note_redelivery(self, fingerprint: str) -> bool:
        """已知毒丸被重新投递时调用：只累加次数，不再解析、不再写 DLQ。

        返回 True 表示命中台账（调用方应直接跳过该消息）。
        计数同样追加落盘，重启后可见完整重投递次数。
        """
        existing = self._entries.get(fingerprint)
        if existing is None:
            return False
        existing.occurrences += 1
        self._append(existing)
        return True

    def _append(self, entry: PoisonEntry) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict(), ensure_ascii=False) + "\n")

    def all(self) -> list[PoisonEntry]:
        return list(self._entries.values())

    def __len__(self) -> int:
        return len(self._entries)


# ---------------------------------------------------------------- DLQ 下游


class DeadLetterSink(Protocol):
    """死信下游协议。生产实现是 Kafka DLQ producer，测试可替换为失败桩。"""

    def publish(self, letters: list[DeadLetter]) -> None:
        """投递一批死信；下游不可用时必须抛异常，由 DLQ 负责溢出兜底。"""
        ...


class FailingDeadLetterSink:
    """测试桩：模拟 DLQ 下游持续不可用。"""

    def __init__(self) -> None:
        self.published: list[DeadLetter] = []
        self.calls = 0

    def publish(self, letters: list[DeadLetter]) -> None:
        self.calls += 1
        raise RuntimeError("DLQ downstream unavailable")


class InMemoryDeadLetterSink:
    """测试 / 本地验证用：收集死信到内存。"""

    def __init__(self) -> None:
        self.published: list[DeadLetter] = []

    def publish(self, letters: list[DeadLetter]) -> None:
        self.published.extend(letters)


# ---------------------------------------------------------------- DLQ 核心


@dataclass
class DLQStats:
    accepted: int = 0          # 首次接收并落盘的死信
    duplicate_poison: int = 0  # 命中毒丸台账、直接跳过的重投递
    publish_failures: int = 0  # 下游发布失败的批次数
    spilled: int = 0           # 溢出到本地文件的死信数
    drained: int = 0           # 下游恢复后补投成功的死信数
    by_category: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "duplicate_poison": self.duplicate_poison,
            "publish_failures": self.publish_failures,
            "spilled": self.spilled,
            "drained": self.drained,
            "by_category": dict(self.by_category),
        }


class DeadLetterQueue:
    """死信队列：毒丸台账 + 下游发布 + 本地溢出兜底。

    写入路径（:meth:`accept`）：

    1. 命中毒丸台账 → 计 ``duplicate_poison`` 后跳过，不重复落盘、不发布；
    2. 新毒丸 → 追加写本地 DLQ 文件（事实落盘，重启不丢）并登记台账；
    3. 尝试发布到下游；下游挂了 → 整批进溢出目录，等待 :meth:`drain` 补投。

    本地 DLQ 文件是真相源，下游只是分发通道，因此下游故障不影响
    "坏消息已被隔离且可复现"这一基本保证。
    """

    def __init__(
        self,
        root: Path,
        downstream: DeadLetterSink | None = None,
        *,
        max_error_chars: int = 500,
    ) -> None:
        self.root = root
        self.dlq_dir = root / "dead_letters"
        self.spill_dir = self.dlq_dir / "_spill"
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        self.spill_dir.mkdir(parents=True, exist_ok=True)
        self.downstream = downstream or InMemoryDeadLetterSink()
        self.max_error_chars = max_error_chars
        self.ledger = PoisonLedger(self.dlq_dir / "poison_ledger.jsonl")
        self.stats = DLQStats()
        # 恢复后先重建台账，溢出文件在 drain 时补投。
        self._spill_seq = self._next_spill_seq()

    # ---------- 接收 ----------

    def accept(self, letter: DeadLetter) -> bool:
        """接收一条死信。返回 False 表示命中台账被跳过（重复毒丸）。"""
        if self.ledger.is_poison(letter.fingerprint):
            self.stats.duplicate_poison += 1
            self.ledger.record(letter)  # 只累加 occurrences，不重复落盘
            return False

        letter.error_summary = letter.error_summary[: self.max_error_chars]
        self._append(letter)
        self.ledger.record(letter)
        self.stats.accepted += 1
        self.stats.by_category[letter.failure_category] = (
            self.stats.by_category.get(letter.failure_category, 0) + 1
        )

        # 下游发布失败不改变"已隔离"事实：溢出后等 drain。
        try:
            self.downstream.publish([letter])
        except Exception:
            self.stats.publish_failures += 1
            self._spill([letter])
        return True

    def accept_batch(self, letters: list[DeadLetter]) -> dict[str, int]:
        """批量接收。单条/单批失败互相隔离，返回各类计数。

        与逐条 :meth:`accept` 的区别：下游按批发布，整批失败时一次溢出，
        避免异常流量把下游打成逐条请求。
        """
        fresh: list[DeadLetter] = []
        accepted = 0
        duplicate = 0
        for letter in letters:
            if self.ledger.is_poison(letter.fingerprint):
                duplicate += 1
                self.stats.duplicate_poison += 1
                self.ledger.record(letter)
                continue
            letter.error_summary = letter.error_summary[: self.max_error_chars]
            self._append(letter)
            self.ledger.record(letter)
            fresh.append(letter)
            accepted += 1
            self.stats.accepted += 1
            self.stats.by_category[letter.failure_category] = (
                self.stats.by_category.get(letter.failure_category, 0) + 1
            )

        if fresh:
            try:
                self.downstream.publish(fresh)
            except Exception:
                self.stats.publish_failures += 1
                self._spill(fresh)
        return {"accepted": accepted, "duplicate": duplicate}

    # ---------- 下游恢复后的补投 ----------

    def drain(self) -> int:
        """把溢出目录里的死信补投到下游。

        下游仍不可用时保持溢出文件不动并返回 0，可周期性重试；
        补投成功的文件才删除（先发布成功、后删文件，至少一次投递）。
        """
        drained = 0
        for path in sorted(self.spill_dir.glob("spill-*.jsonl")):
            letters = [DeadLetter(**d) for d in _read_jsonl(path)]
            if not letters:
                path.unlink(missing_ok=True)
                continue
            try:
                self.downstream.publish(letters)
            except Exception:
                # 下游还没好：保留文件，停止本轮，避免无意义重试风暴。
                self.stats.publish_failures += 1
                break
            path.unlink(missing_ok=True)
            drained += len(letters)
        self.stats.drained += drained
        return drained

    # ---------- 读取 / 观测 ----------

    def read_all(self) -> list[DeadLetter]:
        """读取全部已落盘死信（不含溢出目录里的重复副本）。"""
        path = self.dlq_dir / "dead-letters.jsonl"
        if not path.exists():
            return []
        return [DeadLetter(**d) for d in _read_jsonl(path)]

    def spill_count(self) -> int:
        return sum(
            sum(1 for line in p.open(encoding="utf-8") if line.strip())
            for p in self.spill_dir.glob("spill-*.jsonl")
        )

    # ---------- 内部 ----------

    def _append(self, letter: DeadLetter) -> None:
        path = self.dlq_dir / "dead-letters.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(letter.to_dict(), ensure_ascii=False) + "\n")

    def _spill(self, letters: list[DeadLetter]) -> None:
        seq = self._spill_seq
        self._spill_seq += 1
        path = self.spill_dir / f"spill-{seq:06d}.jsonl"
        with path.open("w", encoding="utf-8") as fh:
            for letter in letters:
                fh.write(json.dumps(letter.to_dict(), ensure_ascii=False) + "\n")
        self.stats.spilled += len(letters)

    def _next_spill_seq(self) -> int:
        existing = [int(p.stem.split("-")[1]) for p in self.spill_dir.glob("spill-*.jsonl")]
        return (max(existing) + 1) if existing else 0


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.append(json.loads(line))
    return out


# ---------------------------------------------------------------- 工厂


def make_dead_letter(
    msg: RawMessage,
    exc: DecodeError,
    *,
    attempt: int | None = None,
) -> DeadLetter:
    """从解码失败构造死信，统一补齐指纹、版本与处理时间。"""
    fingerprint = message_fingerprint(msg.topic, msg.partition, msg.offset, msg.raw)
    return DeadLetter(
        raw_payload=msg.raw,
        failure_category=exc.category.value,
        error_summary=str(exc),
        schema_version=exc.version,
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        failed_at=_utcnow_iso(),
        attempt=msg.delivery_attempt if attempt is None else attempt,
        fingerprint=fingerprint,
        event_id=exc.event_id,
    )


def wrap_event(payload: dict[str, Any], *, partition: int = 0, offset: int = 0) -> RawMessage:
    """把已经是 dict 的事件包成 :class:`RawMessage`（测试 / 回放用）。"""
    return RawMessage(
        raw=json.dumps(payload, ensure_ascii=False),
        partition=partition,
        offset=offset,
    )


def redeliver(msg: RawMessage) -> RawMessage:
    """模拟上游重投递：内容与定位三元组不变，投递序号 +1。

    Kafka 在消费者位移未提交时重平衡/重启会原样重放同一条消息，
    因此 fingerprint 保持不变——这正是毒丸台账能拦住它的原因。
    """
    return RawMessage(
        raw=msg.raw,
        topic=msg.topic,
        partition=msg.partition,
        offset=msg.offset,
        delivery_attempt=msg.delivery_attempt + 1,
    )
