# streamforge

实时湖仓入湖作业。消费 `order_events`，清洗后写 Iceberg，供下游分钟级看板与风控使用。

**当前状态：能跑，但不重不漏、迟到不丢这两件事都不成立。**

## 快速开始

```bash
pip install -r requirements.txt

python -m streamforge replay --n 5000            # 生成事件流到 data/events.jsonl
python -m streamforge run                        # 跑一遍作业
python -m streamforge verify                     # 一致性断言
make demo                                        # 固定演示序列
```

不需要 Kafka / Flink / JVM。本仓库自带一个**语义等价的 Python 运行时**，
复刻了事件时间、水位线、侧输出、checkpoint 与两阶段提交这些行为。
真实作业在 `flinkjob/`（Scala 3 + Flink 1.19），两边逐项对应。

## 结构

```
streamforge/
    events.py      事件模型、确定性生成器、ground_truth、JSONL 读写
    job.py         作业运行时（Watermarks / DimState / Sink / Job）
    config.py      运行参数（全部可由环境变量覆盖）
    __main__.py    CLI: replay / run / verify / dim-reload
flinkjob/          生产实现：Scala 3 + Flink 1.19 + Kafka + Iceberg
data/              replay 生成的事件流
warehouse/         落表结果（orders/、late_events/、_checkpoints/、_staging/）
tests/
scripts/
```

## 两种时间语义

`OrderEvent` 同时带 `event_time`（业务发生）与 `ingest_time`（到达 Kafka）。
作业用哪个字段决定一切：

| 配置 | 用什么时间 | 迟到数据 | 后果 |
|---|---|---|---|
| `PROCESSING_TIME=1`（默认） | `ingest_time` | 被判为正常，静默吸收 | 按 `event_time` 汇总时缺数 |
| `PROCESSING_TIME=0` | `event_time` + 水位线 | 进水侧输出 | 正确 |

实测（3000 事件、4% 迟到率）：处理时间模式下 128 条迟到事件全部被吸收，侧输出 0 条。

## 两种提交语义

| 配置 | 崩溃后重启 | 结果 |
|---|---|---|
| `TWO_PHASE_COMMIT=0`（默认） | 已写入的批次无法回滚 | **重复**（实测 800 条重复） |
| `TWO_PHASE_COMMIT=1` | staging 被 abort，从 checkpoint 重放 | 恰好一次（实测 3000 = 3000） |

```bash
# 复现重复
SINK_ROOT=/tmp/a python -m streamforge verify --crash-at 800

# 复现正确
SINK_ROOT=/tmp/b TWO_PHASE_COMMIT=1 python -m streamforge verify --crash-at 800
```

## 关键指标

`verify` 输出六项断言：

| 断言 | 含义 |
|---|---|
| `rows_no_dup` | 落表行数 = 事件条数 |
| `no_dup_events` | 没有重复 event_id |
| `orders_complete` | 落表订单数 = 真值订单数 |
| `late_events_accounted` | 迟到事件被正确归窗或侧输出 |
| `file_count` | 文件数在阈值内（小文件治理） |
| `dim_loaded` | 维表已加载 |

## 维表

`dim_shop` 上游每 10 分钟推全量快照。作业是 `clear()` + 覆盖，
reload 窗口内查询会落空（`dim.miss_during_reload` 计数）。
用 `python -m streamforge dim-reload` 演示。

## 测试

```bash
python -m pytest -q
```

17 个用例覆盖：生成器确定性、事件排序、ground_truth 口径、水位线单调性与迟到判定、
处理时间吸收迟到、事件时间侧输出、无崩溃无重复、无 2PC 崩溃重复、
2PC 恰好一次、staging 在恢复时被清理、维表 reload、小文件数量、订单口径。
