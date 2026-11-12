"""事件生成器与 Kafka 回放工具。

`replay` 的目标是让任何人都能用同一条 JSONL 复现同一条流。
所有事件的时间戳与乱序程度都由 seed 决定。
"""

from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

CHANNELS = ["app", "web", "mini_program", "api"]
STATUSES = ["created", "paid", "paid", "paid", "cancelled", "refunded"]


@dataclass
class OrderEvent:
    """一条订单事件。

    注意 `event_time` 与 `ingest_time` 的差别：
        event_time   业务发生时间（来自上游系统时钟，可能有偏差）
        ingest_time  到达 Kafka 的时间（processing time 近似）
    这两个字段的混用是本仓库最核心的坑。
    """

    event_id: str
    order_id: str
    shop_id: str
    status: str
    amount_cents: int
    channel: str
    event_time: str      # ISO 8601, UTC
    ingest_time: str     # ISO 8601, UTC

    def event_ts(self) -> datetime:
        return datetime.fromisoformat(self.event_time)

    def ingest_ts(self) -> datetime:
        return datetime.fromisoformat(self.ingest_time)


def generate(
    n: int = 5000,
    seed: int = 20261112,
    late_ratio: float = 0.04,
    base_time: datetime | None = None,
) -> list[OrderEvent]:
    """生成事件流。

    `late_ratio` 的事件会被"延迟送达"：event_time 正常，但 ingest_time 晚 5~30 分钟，
    且它们会排在一批较新的事件之后。这正是水位线要处理的情况。
    """
    rnd = random.Random(seed)
    base = base_time or datetime(2026, 11, 10, 0, 0, 0, tzinfo=timezone.utc)
    events: list[OrderEvent] = []

    for i in range(n):
        offset_s = i * 3  # 每 3 秒一条，约 4 小时的总量
        ev = base + timedelta(seconds=offset_s)
        is_late = rnd.random() < late_ratio
        ingest = ev + timedelta(seconds=rnd.randint(0, 3))
        if is_late:
            ingest = ev + timedelta(minutes=rnd.randint(5, 30))
        events.append(
            OrderEvent(
                event_id=f"E{i:08d}",
                order_id=f"ORD-{rnd.randint(1, n // 3):06d}",
                shop_id=f"SHOP-{rnd.randint(1, 40):03d}",
                status=rnd.choice(STATUSES),
                amount_cents=rnd.randint(100, 500_000),
                channel=rnd.choice(CHANNELS),
                event_time=ev.isoformat(),
                ingest_time=ingest.isoformat(),
            )
        )

    # 按 ingest_time 排序 = Kafka 的到达顺序
    events.sort(key=lambda e: (e.ingest_time, e.event_id))
    return events


def write_jsonl(events: list[OrderEvent], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(asdict(ev), ensure_ascii=False) + "\n")
    return len(events)


def read_jsonl(path: Path) -> list[OrderEvent]:
    out = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                out.append(OrderEvent(**json.loads(line)))
    return out


def ground_truth(events: list[OrderEvent]) -> dict[str, int]:
    """按 order_id 去重后的真值。

    真值口径：每个订单取 event_time 最晚的那条事件（即最终状态）。
    这是断言"不重不漏"的基准。
    """
    latest: dict[str, OrderEvent] = {}
    for ev in events:
        cur = latest.get(ev.order_id)
        if cur is None or ev.event_ts() > cur.event_ts():
            latest[ev.order_id] = ev
    return {oid: ev.amount_cents for oid, ev in latest.items()}
