"""streamforge —— 实时湖仓入湖作业。

一条消费 `order_events` 的 Flink 作业：字段清洗后写 Iceberg，
维表以版本化快照的方式加载进作业状态（构建完成后原子切换）。

本仓库提供两套实现，共享同一套算子语义：

    flinkjob/                 Scala 3 + Flink 1.19 的真实作业（生产）
    streamforge/              Python 实现的等价运行时（本地验证 / CI / 录屏）

模拟器刻意复刻了 Flink 的关键语义，包括：
    * 事件时间（event time）与处理时间（processing time）的区别
    * 水位线（watermark）与迟到数据的侧输出（side output）
    * checkpoint 与两阶段提交（2PC）的差异
    * 维表双缓冲构建与原子版本切换，每行结果带版本标识
    * 状态 TTL 与 RocksDB 的近似行为

这样做的原因：起一个 Flink 集群需要 ~2GB 内存与 30 秒启动，
而本仓库要考察的是**语义正确性**（不重不漏），不是集群运维。
生产配置见 configs/flink-job.yaml。
"""

__version__ = "1.4.0"
