package com.bigdata.rulematch.scala.news.dao

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.bigdata.rulematch.scala.news.beans.rule.EventCombinationCondition
import com.bigdata.rulematch.scala.news.utils.EventUtil
import org.slf4j.{Logger, LoggerFactory}

/**
 * click house查询器
 */
class ClickHouseQuerier {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var ckConn: Connection = null

  def this(ckConn: Connection) = {
    this()
    this.ckConn = ckConn
  }

  /**
   * 在clickhouse中，根据组合条件及查询的时间范围，得到返回结果的1213形式字符串序列
   *
   * queryRangeStart 和 queryRangeEnd这两个参数主要是为分段查询设计的，所以没有直接使用条件中的起止时间
   *
   * @param keyByField                keyby的时候使用的字段名称
   * @param keyByFieldValue           keyby的时候使用的字段对应的值
   * @param eventCombinationCondition 行为组合条件
   * @param queryRangeStart           查询时间范围起始
   * @param queryRangeEnd             查询时间范围结束
   */
  private def getEventCombinationConditionStr(keyByField: String,
                                              keyByFieldValue: String,
                                              eventCombinationCondition: EventCombinationCondition,
                                              queryRangeStart: Long,
                                              queryRangeEnd: Long) = {

    logger.debug("CK收到一个组合查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCombinationCondition)

    //查询出事件Id的过滤SQL语句
    val querySqlStr = eventCombinationCondition.querySql

    val pstmt: PreparedStatement = ckConn.prepareStatement(querySqlStr)

    pstmt.setString(1, keyByFieldValue)
    pstmt.setLong(2, queryRangeStart)
    pstmt.setLong(3, queryRangeEnd)

    //组合条件中要求的所有条件的列表   [A C F]
    val conditionList = eventCombinationCondition.eventConditionList

    //从事件列表中取出事件ID
    val eventIdList: List[String] = conditionList.map(_.eventId)

    val eventIndexSeqBuilder = new StringBuilder

    val rs: ResultSet = pstmt.executeQuery()

    while (rs.next()) {
      //因为只查询了事件Id一个字段，所以直接用列索引号去取就可以
      val eventId = rs.getString(1)

      // 根据eventId到组合条件的事件列表中找对应的索引号，来作为最终结果拼接
      // 如果是原始的事件名称,比如add_cart, order_pay, 进行正则匹配的时候比较麻烦, 所以先 转换成 1212  这种形式
      eventIndexSeqBuilder.append(eventIdList.indexOf(eventId) + 1)
    }

    eventIndexSeqBuilder.toString()
  }

  /**
   * 利用正则表达式查询组合条件满足的次数
   */
  def queryEventCombinationConditionCount(keyByField: String,
                                          keyByFieldValue: String,
                                          eventCombinationCondition: EventCombinationCondition,
                                          queryRangeStart: Long,
                                          queryRangeEnd: Long) = {

    //先查询事件序列
    val eventIndexSeqStr = getEventCombinationConditionStr(keyByField, keyByFieldValue, eventCombinationCondition,
      queryRangeStart, queryRangeEnd)

    //取出组合条件中的正则表达式
    val matchPattern = eventCombinationCondition.matchPattern

    val matchCount = EventUtil.sequenceStrMatchRegexCount(eventIndexSeqStr, matchPattern)

    matchCount
  }
}
