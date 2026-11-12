// 极简 JSON 解码。
//
// 生产环境应使用 jackson-module-scala（build.sbt 里已声明）。
// 这里手写是为了让 `sbt compile` 在没有任何运行期依赖时也能通过，
// 同时保证字段语义与 streamforge/events.py 的 OrderEvent 一致。

package streamforge

import java.time.Instant

object JsonCodec {

  def decodeOrder(json: String): OrderEvent = {
    val m = fields(json)
    OrderEvent(
      eventId = m.getOrElse("event_id", ""),
      orderId = m.getOrElse("order_id", ""),
      shopId = m.getOrElse("shop_id", ""),
      status = m.getOrElse("status", ""),
      amountCents = m.getOrElse("amount_cents", "0").toLong,
      channel = m.getOrElse("channel", ""),
      eventTime = Instant.parse(m.getOrElse("event_time", "1970-01-01T00:00:00Z")),
      ingestTime = Instant.parse(m.getOrElse("ingest_time", "1970-01-01T00:00:00Z"))
    )
  }

  def decodeDim(json: String): DimRow = {
    val m = fields(json)
    DimRow(
      shopId = m.getOrElse("shop_id", ""),
      shopName = m.getOrElse("shop_name", ""),
      region = m.getOrElse("region", ""),
      tier = m.getOrElse("tier", "")
    )
  }

  /** 只处理"平铺、值为字符串或数字"的 JSON 对象。
    *
    * 这够用于本仓库的示例数据；嵌套对象与转义不处理。
    */
  private def fields(json: String): Map[String, String] = {
    val body = json.trim.stripPrefix("{").stripSuffix("}")
    if (body.isBlank) return Map.empty
    val pairs = splitTopLevel(body, ',')
    pairs.flatMap { pair =>
      val kv = splitTopLevel(pair, ':')
      if (kv.length < 2) None
      else {
        val k = kv(0).trim.stripPrefix("\"").stripSuffix("\"")
        val v = kv.drop(1).mkString(":").trim.stripPrefix("\"").stripSuffix("\"")
        Some(k -> v)
      }
    }.toMap
  }

  private def splitTopLevel(s: String, sep: Char): Array[String] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[String]
    val sb = new StringBuilder
    var inStr = false
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c == '"' && (i == 0 || s.charAt(i - 1) != '\\')) inStr = !inStr
      if (c == sep && !inStr) {
        out += sb.toString
        sb.clear()
      } else {
        sb.append(c)
      }
      i += 1
    }
    out += sb.toString
    out.toArray
  }
}
