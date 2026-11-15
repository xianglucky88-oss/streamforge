// 毒消息隔离与死信队列（DLQ）的生产侧骨架。
//
// 与 Python 端 streamforge/poison.py 逐项对应：
//
//   Python 概念                         Flink 概念
//   ---------------------------         --------------------------------------
//   FailureCategory                     DeadLetter.failureCategory（枚举字符串）
//   RawMessage                          Kafka Record<String>（带 topic/partition/offset）
//   decode_message 三级防线             ProcessFunction 内的解码 + 版本 + 契约校验
//   DeadLetter                          OutputTag[DeadLetter] 侧输出记录
//   PoisonLedger                        随 checkpoint 持久化的 MapState<fingerprint, Long>
//   DeadLetterQueue 本地溢出 + drain    DLQ producer 故障时写本地 fallback 文件
//
// 关键语义（两边必须一致）：
//   1. 反序列化失败 / 未知版本 / 契约违例的消息绝不进入主流窗口；
//   2. 死信保留原文、topic/partition/offset、失败类别、schema 版本、处理时间；
//   3. 毒丸指纹进 keyed state，重启后重投递命中即跳过，不无限失败、不重复落 DLQ；
//   4. DLQ 下游不可用时溢出本地文件，恢复后补投，坏消息不丢。
//
// 本文件只给出可编译的骨架与类型定义；Kafka / 对象存储连接器在真实部署里注入。

package streamforge

import java.time.Instant

/** 失败类别。取值与 Python 端 FailureCategory 完全一致，供下游统计与告警。 */
enum FailureCategory(val value: String):
  case DeserializationError extends FailureCategory("deserialization_error")
  case UnknownVersion extends FailureCategory("unknown_version")
  case ContractViolation extends FailureCategory("contract_violation")
  case DlqDownstreamError extends FailureCategory("dlq_downstream_error")

/** 一条死信。字段与 Python 端 DeadLetter 对齐，全部可 JSON 序列化。 */
final case class DeadLetter(
    rawPayload: String,
    failureCategory: String,
    errorSummary: String,
    schemaVersion: Option[String],
    topic: String,
    partition: Int,
    offset: Long,
    failedAt: Instant,
    attempt: Int,
    fingerprint: String,
    eventId: Option[String]
)

/** 解码失败，携带类别与可抢救的版本 / 事件标识。 */
final class DecodeException(
    val category: FailureCategory,
    message: String,
    val schemaVersion: Option[String] = None,
    val eventId: Option[String] = None
) extends RuntimeException(message)

/** 毒消息隔离算子。
  *
  * 主流输出解码成功的 OrderEvent；坏消息走 DLQ 侧输出。
  * 毒丸指纹存 keyed state（这里以指纹为 key），随 checkpoint 持久化，
  * 重启后重投递命中即跳过。
  */
final class PoisonGate(
    supportedVersions: Set[String],
    dlqTag: org.apache.flink.util.OutputTag[DeadLetter]
) extends org.apache.flink.streaming.api.functions.ProcessFunction[String, OrderEvent] {

  // 指纹 -> 已见次数。随 checkpoint 持久化，对应 Python 的 PoisonLedger。
  @transient private var poisonSeen: org.apache.flink.api.common.state.MapState[String, java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    val desc = new org.apache.flink.api.common.state.MapStateDescriptor[String, java.lang.Long](
      "poison-ledger",
      org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[String]),
      org.apache.flink.api.common.typeinfo.TypeInformation.of(classOf[java.lang.Long])
    )
    poisonSeen = getRuntimeContext.getMapState(desc)
  }

  override def processElement(
      raw: String,
      ctx: org.apache.flink.streaming.api.functions.ProcessFunction[String, OrderEvent]#Context,
      out: Collector[OrderEvent]
  ): Unit = {
    val meta = ctx.timestamp() // 真实实现取 KafkaRecordMetaData 的 topic/partition/offset
    val topic = "order_events"
    val partition = ctx.getIndexOfThisSubtask
    val offset = if (meta == null) -1L else meta

    val fingerprint = DeadLetterSupport.fingerprint(topic, partition, offset, raw)

    // 第一道：毒丸台账。命中即跳过，不再解析、不再重复写 DLQ。
    if (poisonSeen.contains(fingerprint)) {
      poisonSeen.put(fingerprint, poisonSeen.get(fingerprint) + 1L)
      return
    }

    try {
      val event = DeadLetterSupport.decode(raw, supportedVersions)
      out.collect(event)
    } catch {
      case e: DecodeException =>
        val letter = DeadLetter(
          rawPayload = raw,
          failureCategory = e.category.value,
          errorSummary = DeadLetterSupport.truncate(e.getMessage, 500),
          schemaVersion = e.schemaVersion,
          topic = topic,
          partition = partition,
          offset = offset,
          failedAt = Instant.now(),
          attempt = 1,
          fingerprint = fingerprint,
          eventId = e.eventId
        )
        poisonSeen.put(fingerprint, 1L) // 登记毒丸，重启后由 state 拦住重投递
        ctx.output(dlqTag, letter)
    }
  }
}

/** 解码与指纹工具，逻辑与 Python 端 decode_message / message_fingerprint 对齐。 */
object DeadLetterSupport {

  /** 稳定指纹：topic/partition/offset + 原文哈希，重启前后不变。 */
  def fingerprint(topic: String, partition: Int, offset: Long, raw: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256")
    md.update(topic.getBytes("UTF-8")); md.update(0x1f.toByte)
    md.update(partition.toString.getBytes("UTF-8")); md.update(0x1f.toByte)
    md.update(offset.toString.getBytes("UTF-8")); md.update(0x1f.toByte)
    md.update(raw.getBytes("UTF-8"))
    md.digest().map(b => f"$b%02x").mkString
  }

  def truncate(s: String, max: Int): String = if (s.length <= max) s else s.substring(0, max)

  /** 三级防线：JSON 反序列化 -> 版本识别 -> 基本契约。
    *
    * 生产环境用 Jackson 的异常类型区分反序列化失败；
    * 这里保留结构与抛出约定，具体 JSON 绑定在部署模块实现。
    */
  def decode(raw: String, supportedVersions: Set[String]): OrderEvent =
    throw new DecodeException(
      FailureCategory.DeserializationError,
      "JSON binding injected in deployment module"
    )
}
