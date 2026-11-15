"""运行参数。"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    """作业配置。"""

    sink_root: Path = field(default_factory=lambda: Path(_env("SINK_ROOT", "warehouse")))
    # 每条事件携带的事件时间允许的最大乱序（秒）。
    # 设成 0 就是"完全按到达顺序"，设成 300 就是"容忍 5 分钟乱序"。
    allowed_lateness_seconds: int = field(default_factory=lambda: int(_env("ALLOWED_LATENESS", "0")))
    # checkpoint 间隔（事件条数）。真实 Flink 是按时间，这里按条数便于复现。
    checkpoint_interval: int = field(default_factory=lambda: int(_env("CHECKPOINT_INTERVAL", "1000")))
    # 维表全量 reload 的间隔（秒）。
    dim_reload_seconds: int = field(default_factory=lambda: int(_env("DIM_RELOAD_SECONDS", "600")))
    # 维表版本的内存预算（字节）。旧版本 + 候选版本之和超过它时拒绝发布。
    dim_max_bytes: int = field(
        default_factory=lambda: int(_env("DIM_MAX_BYTES", str(512 * 1024 * 1024)))
    )
    # 保留的维表版本数（1 = 只留活动版本，2 = 额外留一个可回退的上一版本）。
    dim_retain_versions: int = field(
        default_factory=lambda: max(1, min(2, int(_env("DIM_RETAIN_VERSIONS", "2"))))
    )
    # 状态 TTL（小时）。0 表示不设 TTL。
    state_ttl_hours: int = field(default_factory=lambda: int(_env("STATE_TTL_HOURS", "0")))
    # 是否启用两阶段提交。默认关闭。
    two_phase_commit: bool = field(default_factory=lambda: _env("TWO_PHASE_COMMIT", "0") == "1")
    # 时间语义：True = 处理时间（ingest_time），False = 事件时间（event_time）。
    processing_time: bool = field(default_factory=lambda: _env("PROCESSING_TIME", "1") == "1")
    # 小文件合并阈值（每个批次的最大事件数）。
    batch_size: int = field(default_factory=lambda: int(_env("BATCH_SIZE", "200000")))

    def ensure(self) -> None:
        self.sink_root.mkdir(parents=True, exist_ok=True)

    @property
    def orders(self) -> Path:
        return self.sink_root / "orders"

    @property
    def late(self) -> Path:
        return self.sink_root / "late_events"

    @property
    def checkpoints(self) -> Path:
        return self.sink_root / "_checkpoints"

    @property
    def snapshots(self) -> Path:
        return self.sink_root / "_snapshots"


settings = Settings()
