// 辅助对象：source / sink / 反序列化的构造。
//
// 真实项目里这些通常会抽成独立模块。这里保持"能看懂、能编译"的骨架。

package streamforge

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import java.time.Instant

object KafkaSources {

  /** 订单事件源（JSON 字符串）。类型是 String，下游再 map 成 OrderEvent。 */
  def orderEvents(topic: String, bootstrap: String, group: String): KafkaSource[String] =
    KafkaSource
      .builder[String]()
      .setBootstrapServers(bootstrap)
      .setTopics(topic)
      .setGroupId(group)
      .setStartingOffsets(OffsetsInitializer.committedOffsets())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

  /** 维表变更流。上游每 10 分钟推一次全量快照。 */
  def dimUpdates(topic: String, bootstrap: String, group: String): KafkaSource[String] =
    KafkaSource
      .builder[String]()
      .setBootstrapServers(bootstrap)
      .setTopics(topic)
      .setGroupId(s"$group-dim")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()
}

/** JSON -> OrderEvent。生产用 Jackson，这里用最简解析避免多引入依赖。 */
class JsonToOrderEvent extends MapFunction[String, OrderEvent] {
  override def map(json: String): OrderEvent =
    JsonCodec.decodeOrder(json)
}

/** JSON -> DimRow。 */
class JsonToDimRow extends MapFunction[String, DimRow] {
  override def map(json: String): DimRow =
    JsonCodec.decodeDim(json)
}

object IcebergSinks {

  /** Iceberg 表 sink。
    *
    * 注意：要拿到 exactly-once，需要
    *   1. env 启用 EXACTLY_ONCE checkpointing
    *   2. Iceberg 表开启 write.distribution-mode
    *   3. 使用 FlinkSink.forRowData(...).tableLoader(...) 的二阶段提交
    *
    * 当前实现只做了最基础的 streaming write。
    */
  def orders(table: String): SinkFunction[OrderRow] = new SinkFunction[OrderRow] {
    override def invoke(row: OrderRow, context: SinkFunction.Context): Unit = {
      // 实际实现会调用 Iceberg 的 FlinkSink；这里留桩，避免把仓库变成不可编译的大工程。
      ()
    }
  }
}

object LateSinks {
  def jsonl(prefix: String): SinkFunction[OrderEvent] = new SinkFunction[OrderEvent] {
    override def invoke(e: OrderEvent, context: SinkFunction.Context): Unit = {
      // 写到对象存储的 `${prefix}/date=.../part-*.jsonl`
      ()
    }
  }
}
