package com.bigdata.rulematch.scala.datagen

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.alibaba.fastjson.JSON
import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import com.bigdata.rulematch.scala.utils.ConnectionUtils
import org.apache.commons.dbutils.DbUtils
import org.apache.commons.lang3.StringUtils


/**
 *
 * 数据明细模拟数据生成插入 ClickHouse
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 0:14
 */
object ClickHouseDataMock {

  def main(args: Array[String]): Unit = {
    //插入数据测试
    val jsonStr = "{\"userId\":\"u202112180001\",\"timeStamp\":1639839496220,\"eventId\":\"productView\",\"properties\":{\"pageId\":\"646\",\"productId\":\"157\",\"title\":\"爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码\",\"url\":\"https://item.jd.com/36506691363.html\"}}"

    val eventLogBean: EventLogBean = JSON.parseObject(jsonStr, classOf[EventLogBean])

    //eventLogDataToClickHouse(eventLogBean)

    sequenceMatchTest
  }

  /**
   * 将 EventLogBean 数据插入 ClickHouse
   * @param eventLogBean
   */
  def eventLogDataToClickHouse(eventLogBean: EventLogBean) = {

    val conn: Connection = ConnectionUtils.getClickHouseConnection()

    val sqlStr =
      s"""
        |INSERT INTO ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
        |(userId,eventId,timeStamp,properties) VALUES(?,?,?,?)
        |""".stripMargin

    val pstmt = conn.prepareStatement(sqlStr)

    var jsonStr = JSON.toJSON(eventLogBean.properties).toString

    //只是为了测试, value中不要带特殊符号，尤其是 "
    jsonStr = StringUtils.replace(jsonStr, "\"", "'")

    pstmt.setString(1, eventLogBean.userId)
    pstmt.setString(2, eventLogBean.eventId)
    pstmt.setLong(3, eventLogBean.timeStamp)
    pstmt.setString(4, jsonStr)

    pstmt.execute()

    println(s"插入 click house : ${jsonStr}")

    DbUtils.closeQuietly(conn, pstmt, null)

    println("[click house],  关闭连接")
  }

  /**
   * 路径匹配函数测试, 这个SQL是进行行为次序类规则匹配的关键
   */
  def sequenceMatchTest() = {

    /**
     * 语法：
     * sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
     *
     * 参数：
     * pattern — 模式字符串
     * --->模式语法
     * (?N) — 在位置N匹配条件参数。 条件在编号 [1, 32] 范围。 例如, (?1) 匹配传递给 cond1 参数。
     * .* — 表示任意的非指定事件
     * (?t operator value) — 分开两个事件的时间。 例如： (?1)(?t>1800)(?2) 匹配彼此发生超过1800秒的事件。
     * 这些事件之间可以存在任意数量的任何事件。 您可以使用 >=, >, <, <=, == 运算符。
     * (?1)(?t<=15)(?2)即表示事件1和2发生的时间间隔在15秒以内。
     *
     * timestamp — 包含时间的列。典型的时间类型是： Date 和 DateTime。您还可以使用任何支持的 UInt 数据类型。
     * cond1, cond2 — 事件链的约束条件。 数据类型是： UInt8。 最多可以传递32个条件参数。
     * 该函数只考虑这些条件中描述的事件。 如果序列包含未在条件中描述的数据，则函数将跳过这些数据
     *
     * 下面的 .*(?1).*(?2).*(?3) 表示事件1，2，3前后可以有任意事件，也就是保证先后顺序即可，不需要紧密相邻
     *
     * 返回值:
     * 1，如果模式匹配。
     * 0，如果模式不匹配。
     *
     * 注意：
     * Aggregate function sequenceMatch requires at least 3 arguments
     * sequenceMatch至少需要3个参数，所以is_match1中多给了一个参数
     */
    val querySqlStr =
      s"""
        |SELECT
        |    userId,
        |    sequenceMatch('.*(?1).*(?2).*(?3)')(
        |    toDateTime(`timeStamp`),
        |    eventId = 'adShow' AND properties['adId']='10',
        |    eventId = 'addCart' AND properties['pageId']='720',
        |    eventId = 'collect' AND properties['pageId']='263'
        |   ) AS is_match3,
        |  sequenceMatch('.*(?1).*(?2)')(
        |    toDateTime(`timeStamp`),
        |    eventId = 'adShow' AND properties['adId']='10',
        |    eventId = 'addCart' AND properties['pageId']='720'
        |  ) AS is_match2,
        | sequenceMatch('.*(?1).*')(
        |    toDateTime(`timeStamp`),
        |    eventId = 'adShow' AND properties['adId']='10',
        |    eventId = 'addCart' AND properties['pageId']='720'
        |  ) AS is_match1
        |FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
        |WHERE userId='u202112180001' AND  `timeStamp` > 1639756800000
        |  AND (
        |        (eventId='adShow' AND properties['adId']='10')
        |        OR
        |        (eventId = 'addCart' AND properties['pageId']='720')
        |        OR
        |        (eventId = 'collect' AND properties['pageId']='263')
        |    )
        |GROUP BY userId
        |""".stripMargin

    println(querySqlStr)

    val conn: Connection = ConnectionUtils.getClickHouseConnection()

    val pstmt: PreparedStatement = conn.prepareStatement(querySqlStr)

    val rs: ResultSet = pstmt.executeQuery()

    while(rs.next()){
      val userId = rs.getString("userId")
      val isMatch3 = rs.getInt("is_match3")
      val isMatch2 = rs.getInt("is_match2")
      val isMatch1 = rs.getInt("is_match1")

      println(s"userId: ${userId}, isMatch3: ${isMatch3}, isMatch2: ${isMatch2}, isMatch1: ${isMatch1}")
    }

    DbUtils.closeQuietly(conn, pstmt, rs)

  }
}
