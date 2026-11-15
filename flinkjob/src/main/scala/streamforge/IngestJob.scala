// 生产实现：Scala 3 + Flink 1.19 + Kafka + Iceberg。
//
// 这个文件不在本地验证路径上（起集群成本高），
// 但它与 streamforge/job.py 的语义逐项对应，出题时应保持两边一致：
//
//   Python 概念                      Flink 概念
//   ---------------------------     ------------------------------------
//   Job._consume                    KeyedProcessFunction / ProcessFunction
//   Watermarks                      BoundedOutOfOrdernessWatermarks
//   late_sink                       OutputTag[OrderEvent]("late")
//   DimTable 双缓冲原子切换           EnrichWithVersionedDim + 广播状态"代"隔离
//   Sink 2PC                        FlinkKafkaProducer / Iceberg 的 2PC commit
//   checkpoint_interval              env.enableCheckpointing
//   row.dim_version                  OrderRow.dimVersion / dimDigest
//
// 生产配置与本地模拟器共享同一套语义。
//
// 构建方式见 build.sbt。本项目用 sbt 编译，不依赖本地 DuckDB 路径。

package streamforge

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{BroadcastStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.Duration

/** 订单事件。字段与 Python 版 events.py 的 OrderEvent 对齐。 */
final case class OrderEvent(
    eventId: String,
    orderId: String,
    shopId: String,
    status: String,
    amountCents: Long,
    channel: String,
    eventTime: java.time.Instant,
    ingestTime: java.time.Instant
)

/** 维表行。 */
final case class DimRow(shopId: String, shopName: String, region: String, tier: String)

/** 落表行。字段与 Python 版 job.py 的 row 对齐。 */
final case class OrderRow(
    eventId: String,
    orderId: String,
    shopId: String,
    shopName: String,
    region: String,
    status: String,
    amountCents: Long,
    channel: String,
    eventTime: java.time.Instant,
    ingestTime: java.time.Instant,
    windowDate: String,
    // 维度版本溯源：每条结果都能证明自己用的是哪份维表。
    dimVersion: String,
    dimDigest: String
)

object IngestJob {

  val LateTag = new org.apache.flink.util.OutputTag[OrderEvent]("late-events")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 当前以 AT_LEAST_ONCE 运行。
    env.enableCheckpointing(60_000L, CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30_000L)

    // 订单流：Kafka JSON -> OrderEvent
    val kafkaSource: org.apache.flink.streaming.api.datastream.DataStream[OrderEvent] =
      env.fromSource(
        KafkaSources.orderEvents("order_events", "localhost:9092", "streamforge"),
        WatermarkStrategy.noWatermarks[String](),
        "order-events"
      ).map(new JsonToOrderEvent)

    // 时间戳取自 ingestTime。
    val withTimestamps: SingleOutputStreamOperator[OrderEvent] =
      kafkaSource.assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[OrderEvent](Duration.ofMinutes(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[OrderEvent] {
            override def extractTimestamp(e: OrderEvent, recordTs: Long): Long =
              e.ingestTime.toEpochMilli
          })
      )

    // 维表流：Kafka JSON -> DimMessage（含快照结束标记），然后广播
    val dimStream: BroadcastStream[DimMessage] =
      env.fromSource(
          KafkaSources.dimUpdates("dim_shop", "localhost:9092", "streamforge"),
          WatermarkStrategy.noWatermarks[String](),
          "dim-shop"
        )
        .map(new JsonToDimMessage)
        .broadcast(DimStateDescriptors.rowsByGen, DimStateDescriptors.countByGen,
          DimStateDescriptors.activeGen)

    val enriched: SingleOutputStreamOperator[OrderRow] = withTimestamps
      .connect(dimStream)
      .process(new EnrichWithVersionedDim)

    // 侧输出：迟到事件。
    enriched
      .getSideOutput(IngestJob.LateTag)
      .addSink(LateSinks.jsonl("late_events"))

    enriched.addSink(IcebergSinks.orders("local.dwd.order_events"))

    env.execute("streamforge-ingest")
  }
}

// 维表广播 + 事实流 join 已改为版本化实现，见 DimVersioning.scala：
//   EnrichWithVersionedDim 按"代"隔离快照，完整校验后原子切换 active gen。
//
// 迟到判定的注意点保持不变：BroadcastProcessFunction 的 ReadOnlyContext
// **没有** timerService()/watermark，迟到过滤必须放在 connect(broadcast)
// 之前的 keyed 算子里，否则恒为 false。
