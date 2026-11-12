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
//   DimState.reload 全量 reload       BroadcastProcessFunction + 全量状态覆盖
//   Sink 2PC                        FlinkKafkaProducer / Iceberg 的 2PC commit
//   checkpoint_interval              env.enableCheckpointing
//
// 生产配置与本地模拟器共享同一套语义。
//
// 构建方式见 build.sbt。本项目用 sbt 编译，不依赖本地 DuckDB 路径。

package streamforge

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.{BroadcastStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

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

/** 落表行。 */
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
    windowDate: String
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

    // 维表流：Kafka JSON -> DimRow，然后广播
    val dimStream: BroadcastStream[DimRow] =
      env.fromSource(
          KafkaSources.dimUpdates("dim_shop", "localhost:9092", "streamforge"),
          WatermarkStrategy.noWatermarks[String](),
          "dim-shop"
        )
        .map(new JsonToDimRow)
        .broadcast(DimDescriptors.dimState)

    val enriched: SingleOutputStreamOperator[OrderRow] = withTimestamps
      .connect(dimStream)
      .process(new EnrichWithDim)

    // 侧输出：迟到事件。
    enriched
      .getSideOutput(IngestJob.LateTag)
      .addSink(LateSinks.jsonl("late_events"))

    enriched.addSink(IcebergSinks.orders("local.dwd.order_events"))

    env.execute("streamforge-ingest")
  }
}

/** 维表广播 + 事实流 join。
  *
  * 上游每 10 分钟推一次全量快照。
  */
final class EnrichWithDim extends BroadcastProcessFunction[OrderEvent, DimRow, OrderRow] {

  override def processElement(
      e: OrderEvent,
      ctx: BroadcastProcessFunction[OrderEvent, DimRow, OrderRow]#ReadOnlyContext,
      out: Collector[OrderRow]
  ): Unit = {
    val state = ctx.getBroadcastState(DimDescriptors.dimState)

    // 迟到判定。
    //
    // 注意：BroadcastProcessFunction 的 ReadOnlyContext **没有** timerService()，
    // 广播流本身不支持定时器（这是 Flink 的设计约束）。
    // 因此真正的迟到判定必须放在 broadcast 之前的 keyed 算子里：
    //   stream.keyBy(...).process(new LateFilter) 再 connect(broadcast)
    //
    // 当前实现把"迟到判定"挂在 broadcast 之后，拿不到 watermark，
    // 因此这里的迟到判定恒为 false。

    val dim: DimRow = if (state.contains(e.shopId)) state.get(e.shopId) else null
    val name = if (dim == null) null else dim.shopName
    val region = if (dim == null) null else dim.region

    out.collect(
      OrderRow(
        eventId = e.eventId,
        orderId = e.orderId,
        shopId = e.shopId,
        shopName = name,
        region = region,
        status = e.status,
        amountCents = e.amountCents,
        channel = e.channel,
        eventTime = e.eventTime,
        ingestTime = e.ingestTime,
        // 窗口归属使用 ingestTime
        windowDate = e.ingestTime.toString.take(10)
      )
    )
  }

  override def processBroadcastElement(
      d: DimRow,
      ctx: BroadcastProcessFunction[OrderEvent, DimRow, OrderRow]#Context,
      out: Collector[OrderRow]
  ): Unit = {
    ctx.getBroadcastState(DimDescriptors.dimState).put(d.shopId, d)
  }
}

object DimDescriptors {
  import org.apache.flink.api.common.state.MapStateDescriptor

  val dimState: MapStateDescriptor[String, DimRow] =
    new MapStateDescriptor[String, DimRow](
      "dim-shop",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[DimRow])
    )
}
