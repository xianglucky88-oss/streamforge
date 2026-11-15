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
    # 状态 TTL（小时）。0 表示不设 TTL。
    state_ttl_hours: int = field(default_factory=lambda: int(_env("STATE_TTL_HOURS", "0")))
    # 是否启用两阶段提交。默认关闭。
    two_phase_commit: bool = field(default_factory=lambda: _env("TWO_PHASE_COMMIT", "0") == "1")
    # 时间语义：True = 处理时间（ingest_time），False = 事件时间（event_time）。
    processing_time: bool = field(default_factory=lambda: _env("PROCESSING_TIME", "1") == "1")
    # 小文件合并阈值（每个批次的最大事件数）。
    batch_size: int = field(default_factory=lambda: int(_env("BATCH_SIZE", "200000")))
    # 对象存储延迟可见模拟：写入后经过多少个可见性时钟才对 list/get 可见。
    # 0 = 立即可见；调大用于在本地复现 S3 list-bucket 的延迟一致性。
    visibility_lag_ticks: int = field(default_factory=lambda: int(_env("VISIBILITY_LAG_TICKS", "0")))
    # 可见性 / settle 窗口的最大重试轮数。
    visibility_attempts: int = field(default_factory=lambda: int(_env("VISIBILITY_ATTEMPTS", "10")))
    # 恢复锁租约（秒）：租约过期后新协调器可接管并使 epoch +1。
    recovery_lease_seconds: float = field(
        default_factory=lambda: float(_env("RECOVERY_LEASE_SECONDS", "30"))
    )
    # GC 宽限（墙钟秒）：事务在 journal 有结论后至少保留多久才允许物理删除。
    reconcile_grace_seconds: float = field(
        default_factory=lambda: float(_env("RECONCILE_GRACE_SECONDS", "3600"))
    )

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
