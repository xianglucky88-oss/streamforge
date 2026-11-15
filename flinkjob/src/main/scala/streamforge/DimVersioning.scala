// 维表版本切换：双缓冲 + 原子启用 + 内容寻址。
//
// 与 streamforge/dim.py 的 Python 模拟器逐项对应：
//
//   Python                              Flink
//   ----------------------------------  ---------------------------------------
//   DimVersion（冻结 rows + digest）    DimSnapshot（gen = 版本 id）
//   DimTable.current                    BroadcastState 里的 "#active"
//   构建完整快照后换一次引用             写满一代 + 校验后 put("#active", gen)
//   _dim_versions/ 快照                 BroadcastState（随 checkpoint 一起恢复）
//   row.dim_version / dim_digest        OrderRow.dimVersion / dimDigest
//
// 广播流的关键约束：processBroadcastElement 会为每条记录写入广播状态。
// 旧实现直接 put(shopId, row)，下游在两次 put 之间读到的是"半新半旧"。
// 这里用"代"（generation）隔离：
//
//   1. 上游全量快照附带 gen（内容摘要 + 递增序号）；
//   2. 普通行写入 `${gen}|${shopId}` 命名空间；
//   3. 收到 `_end` 标记且行数/摘要校验通过后，才 put("#active", gen)；
//   4. processElement 只读 "#active" 指向的那一代，一次查询绝不跨代。
//
// 失败（行数不足、摘要不符、加载中断）时 "#active" 保持不变，旧代继续服务。
// 未完成的代随 checkpoint 一起回滚，不会污染恢复后的新版本。

package streamforge

import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** 广播的维表消息。
  *
  * @param gen          快照版本 id
  * @param row          普通行；`end=true` 时忽略
  * @param end          快照结束标记
  * @param expectRows   结束标记声明的行数
  * @param expectDigest 结束标记声明的内容摘要
  */
final case class DimMessage(
    gen: String,
    row: DimRow = null,
    end: Boolean = false,
    expectRows: Long = -1L,
    expectDigest: String = ""
)

object DimVersionCodec {
  /** 与 Python `canonical_row` 的字段顺序一致（键按字典序、无多余空白）。
    *
    * 这里手写是为了不引入运行期依赖；正式实现应复用与 Python 同一套规范化
    * JSON 序列化（jackson），并对引号/反斜杠/非 ASCII 做一致转义。
    */
  def canonical(row: DimRow): String =
    s"""{"region":"${row.region}","shop_id":"${row.shopId}","shop_name":"${row.shopName}","tier":"${row.tier}"}"""

  /** 按 shop_id 排序后逐行哈希；跨语言一致。 */
  def digest(rows: Iterable[DimRow]): String = {
    val md = MessageDigest.getInstance("SHA-256")
    rows.toSeq.sortBy(_.shopId).foreach { r =>
      md.update(canonical(r).getBytes(StandardCharsets.UTF_8))
      md.update('\n'.toByte)
    }
    md.digest().map(b => f"$b%02x").mkString
  }

  def versionId(seq: Long, digest: String): String = f"dim-$seq%06d-${digest.take(12)}"
}

object DimStateDescriptors {
  /** `${gen}|${shopId}` -> DimRow。 */
  val rowsByGen: MapStateDescriptor[String, DimRow] =
    new MapStateDescriptor[String, DimRow](
      "dim-rows-by-gen",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[DimRow])
    )

  /** gen -> 已写入行数。 */
  val countByGen: MapStateDescriptor[String, java.lang.Long] =
    new MapStateDescriptor[String, java.lang.Long](
      "dim-count-by-gen",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[java.lang.Long])
    )

  /** 单键 "#active" -> 当前生效的 gen。 */
  val activeGen: MapStateDescriptor[String, String] =
    new MapStateDescriptor[String, String](
      "dim-active-gen",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[String])
    )

  val activeKey: String = "#active"
}

/** 维表版本化广播 + 事实流 join。
  *
  * 迟到判定不在这里做：BroadcastProcessFunction 的只读上下文拿不到
  * timerService/watermark，迟到过滤必须在 connect(broadcast) 之前完成。
  */
final class EnrichWithVersionedDim extends BroadcastProcessFunction[OrderEvent, DimMessage, OrderRow] {

  override def processElement(
      e: OrderEvent,
      ctx: BroadcastProcessFunction[OrderEvent, DimMessage, OrderRow]#ReadOnlyContext,
      out: Collector[OrderRow]
  ): Unit = {
    val state: ReadOnlyBroadcastState[String, DimRow] =
      ctx.getBroadcastState(DimStateDescriptors.rowsByGen)
    val genState: ReadOnlyBroadcastState[String, String] =
      ctx.getBroadcastState(DimStateDescriptors.activeGen)
    // 只读一次 gen：整条事件都绑定到同一代维表。
    val gen = genState.get(DimStateDescriptors.activeKey)
    val dim = if (gen == null) null else state.get(s"$gen|${e.shopId}")

    out.collect(
      OrderRow(
        eventId = e.eventId,
        orderId = e.orderId,
        shopId = e.shopId,
        shopName = if (dim == null) null else dim.shopName,
        region = if (dim == null) null else dim.region,
        status = e.status,
        amountCents = e.amountCents,
        channel = e.channel,
        eventTime = e.eventTime,
        ingestTime = e.ingestTime,
        windowDate = e.ingestTime.toString.take(10),
        // 版本溯源：每行都能回答"用了哪份维表"。
        dimVersion = gen,
        dimDigest = if (gen == null) null else gen.split("-").lastOption.orNull
      )
    )
  }

  override def processBroadcastElement(
      m: DimMessage,
      ctx: BroadcastProcessFunction[OrderEvent, DimMessage, OrderRow]#Context,
      out: Collector[OrderRow]
  ): Unit = {
    val state = ctx.getBroadcastState(DimStateDescriptors.rowsByGen)
    val counts = ctx.getBroadcastState(DimStateDescriptors.countByGen)
    val gens = ctx.getBroadcastState(DimStateDescriptors.activeGen)

    if (!m.end) {
      // 只写本代命名空间；"#active" 仍指向旧代，读者看不到半成品。
      state.put(s"${m.gen}|${m.row.shopId}", m.row)
      val n = Option(counts.get(m.gen)).map(_.longValue()).getOrElse(0L) + 1L
      counts.put(m.gen, java.lang.Long.valueOf(n))
      return
    }

    // 结束标记：先校验完整，再一次性切换。
    val n = Option(counts.get(m.gen)).map(_.longValue()).getOrElse(0L)
    if (m.expectRows >= 0 && n != m.expectRows) {
      DimMetrics.failedSnapshot(m.gen, s"row_count $n != ${m.expectRows}")
      return
    }
    if (m.expectDigest.nonEmpty) {
      val builder = Vector.newBuilder[DimRow]
      val it = state.iterator()
      val prefix = s"${m.gen}|"
      while (it.hasNext) {
        val entry = it.next()
        if (entry.getKey.startsWith(prefix)) builder += entry.getValue
      }
      val actual = DimVersionCodec.digest(builder.result())
      if (actual != m.expectDigest) {
        DimMetrics.failedSnapshot(m.gen, s"digest $actual != ${m.expectDigest}")
        return
      }
    }

    // 原子启用：只用一次 put 改变读者看到的代。
    val oldGen = gens.get(DimStateDescriptors.activeKey)
    gens.put(DimStateDescriptors.activeKey, m.gen)
    DimMetrics.activated(m.gen, n)

    // 新代启用后才清理旧代；崩溃恢复时旧代仍会在 checkpoint 里。
    if (oldGen != null && oldGen != m.gen) {
      val prefix = s"$oldGen|"
      val stale = Vector.newBuilder[String]
      val it = state.iterator()
      while (it.hasNext) {
        val key = it.next().getKey
        if (key.startsWith(prefix)) stale += key
      }
      stale.result().foreach { key => state.remove(key) }
      counts.remove(oldGen)
    }
  }
}

/** 观测钩子：生产环境替换为 Flink Metrics。
  *
  * 注意这是 JVM 内共享的计数器，多 subtask 时会叠加；正式实现要用
  * `getRuntimeContext.getMetricGroup` 注册每个 subtask 自己的 gauge。
  */
object DimMetrics {
  @volatile private[streamforge] var activeGen: String = null
  @volatile private[streamforge] var activations: Long = 0L
  @volatile private[streamforge] var failures: Long = 0L
  @volatile private[streamforge] var lastError: String = null

  def activated(gen: String, rows: Long): Unit = {
    activeGen = gen
    activations += 1L
  }

  def failedSnapshot(gen: String, why: String): Unit = {
    failures += 1L
    lastError = s"$gen: $why"
  }
}
