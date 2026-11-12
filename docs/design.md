# 实时入湖链路设计说明

## 目标

订单事件到达后 1 分钟内可查，且保证**不重不漏**：
每个 `event_id` 恰好落一次，迟到事件不被静默丢弃。

## 数据流

```
producers
   │  JSON（详见 streamforge/events.py:OrderEvent）
   ▼
Kafka  order_events ──────────────┐
Kafka  dim_shop（全量快照/10min） ─┤
                                   ▼
                         Flink Job（streamforge/job.py 等价实现）
                                   │
            ┌──────────────────────┼──────────────────────┐
            │                      │                      │
            ▼                      ▼                      ▼
    Iceberg dwd.order_events   late_events/        _checkpoints/
        （事件粒度明细）        （侧输出）          （offset 与水位）
```

## 时间语义

| 字段 | 来源 | 用途 |
|---|---|---|
| `event_time` | 上游系统时钟 | 正确归窗、水位线 |
| `ingest_time` | Kafka 到达时间 | **错误**的归窗口径（当前默认） |

水位线：

```
watermark = max(event_time seen) - allowed_lateness
```

`allowed_lateness` 表示"愿意等多久的乱序"。设得越大，结果越准，但延迟越高。

## 提交语义

### 无 2PC（当前）

```
batch 1 → 写正式目录（立即可见）
batch 2 → 写正式目录
...
崩溃 → 重启 → 从最后 checkpoint 的 offset 重放 → batch 1..N 再写一遍
```

### 有 2PC

```
batch 1 → 写 _staging/
checkpoint #1 完成 → 原子提交 batch 1
batch 2 → 写 _staging/
崩溃 → 重启 → abort 所有 _staging/ → 从 checkpoint #1 的 offset 重放
```

关键点：**checkpoint 完成之前，数据对读方不可见**。

## 幂等性对照表

| 场景 | 无 2PC | 有 2PC |
|---|---|---|
| 正常运行 | 正确 | 正确 |
| 作业崩溃重启 | 重复 | 正确 |
| 上游重发同一事件 | 重复 | 取决于 sink 的去重能力 |
| checkpoint 失败 | 可能部分提交 | 整体回滚 |

## 维表

`dim_shop` 是全量快照流。作业有两种处理方式：

1. **全量覆盖**（当前）：`clear()` + 填充。简单，但有空窗。
2. **双缓冲**：构建新表 → 原子替换。无空窗，但内存峰值翻倍。

生产推荐用 changelog 流（`shop_id` 为主键的 upsert），
这样 Flink 的 `BroadcastState` 只需要 put/remove 单个 key。

## 观察点

| 指标 | 位置 | 正常范围 |
|---|---|---|
| `rows_written` | `verify` → `rows_no_dup` | = 输入事件数 |
| `duplicate_rows` | `no_dup_events` | 0 |
| `late_routed` | `late_events_accounted` | ≈ 迟到事件数 |
| `files` | `file_count` | 与 checkpoint 间隔匹配 |
| `dim.miss_during_reload` | `dim_loaded` | 0 |

## 待决策

1. 水位线容忍度：业务能接受多长的延迟？这决定了 `allowed_lateness_seconds`。
2. 迟到事件的补偿策略：重开窗口 / 增量修正 / 只记录不补偿。
3. Iceberg 表的 `write.distribution-mode`：`hash` 还是 `range`，影响小文件与查询。
4. 状态 TTL：维表状态与聚合状态是否分别设 TTL。
