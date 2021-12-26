package com.bigdata.rulematch.scala.service.impl

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.bigdata.rulematch.scala.bean.rule.{EventCondition, EventSeqCondition}
import org.apache.commons.dbutils.DbUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * ClickHouse查询服务接口实现类
 */
class ClickHouseQueryServiceImpl {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var ckConn: Connection = null

  def this(ckConn: Connection) = {
    this()
    this.ckConn = ckConn
  }

  /**
   * 查询行为次数类
   *
   * @param keyByField
   * @param keyByFieldValue
   * @param eventCondition
   * @return
   */
  def queryActionCountCondition(keyByField: String,
                                keyByFieldValue: String,
                                eventCondition: EventCondition) = {

    queryActionCountCondition(keyByField, keyByFieldValue, eventCondition,
      eventCondition.timeRangeStart, eventCondition.timeRangeEnd)

  }

  def queryActionCountCondition(keyByField: String,
                                keyByFieldValue: String,
                                eventCondition: EventCondition,
                                queryTimeStart: Long,
                                queryTimeEnd: Long) = {

    logger.debug("CK收到一个行为次数类查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCondition)

    val querySqlStr = eventCondition.actionCountQuerySql

    logger.debug(s"构造的clickhouse查询行为次数的sql语句: ${querySqlStr}")

    val pstmt: PreparedStatement = ckConn.prepareStatement(querySqlStr)

    pstmt.setString(1, keyByFieldValue)
    pstmt.setLong(2, queryTimeStart)
    pstmt.setLong(3, queryTimeEnd)

    val rs: ResultSet = pstmt.executeQuery()

    var count = 0L

    while (rs.next()) {
      count = rs.getLong("cnt")

      println(s"查询到的行为次数: ${count}")
    }


    //不能关闭连接Connection,因为需要一直查,连接关闭在RuleMatchKeyedProcessFunction的close方法中
    DbUtils.closeQuietly(null, pstmt, rs)

    //返回查询到的结果
    count
  }

  /**
   * 根据给定的序列类条件, 返回最大完成的步骤号
   * @param keyByField
   * @param keyByFieldValue
   * @param eventSeqCondition
   * @return
   */
  def queryActionSeqCondition(keyByField: String,
                                keyByFieldValue: String,
                                eventSeqCondition: EventSeqCondition) = {

    queryActionSeqCondition(keyByField, keyByFieldValue, eventSeqCondition,
      eventSeqCondition.timeRangeStart, eventSeqCondition.timeRangeEnd)

  }

  /**
   * 根据给定的序列类条件, 返回最大完成的步骤号
   * @param keyByField
   * @param keyByFieldValue
   * @param eventSeqCondition
   * @param queryTimeStart
   * @param queryTimeEnd
   * @return
   */
  def queryActionSeqCondition(keyByField: String,
                              keyByFieldValue: String,
                              eventSeqCondition: EventSeqCondition,
                              queryTimeStart: Long,
                              queryTimeEnd: Long) = {
    logger.debug("CK收到一个行为次序类查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventSeqCondition)

    val querySqlStr = eventSeqCondition.actionSeqQuerySql

    logger.debug(s"构造的clickhouse查询行为次序的sql语句: ${querySqlStr}")

    val pstmt: PreparedStatement = ckConn.prepareStatement(querySqlStr)

    pstmt.setString(1, keyByFieldValue)
    pstmt.setLong(1, queryTimeStart)
    pstmt.setLong(1, queryTimeEnd)

    val rs: ResultSet = pstmt.executeQuery()

    /**
     * ┬─is_match3─┬─is_match2─┬─is_match1─┐
     * │         1 │         1 │         1 │
     * ┴───────────┴───────────┴───────────┘
     */

    //记录最大完成步骤
    var maxStep = 0
    //其实rs中只有一条数据
    if (rs.next()) {

      //如果序列中有3个条件,那么需要依次判断is_match3,is_match2,is_match1是否为1
      //其实也就是 rs.getInt(1),rs.getInt(2),rs.getInt(3)是否为1
      //如果 rs.getInt(1) 等于1, 说明完成了3步(3-0); 如果 rs.getInt(2) 等于1, 说明完成了2步(3-1)
      //序列中有几个条件, is_match的最大值就是几
      var i = 1
      var loopFlag = true
      while (i <= eventSeqCondition.eventSeqList.size && loopFlag) {
        // rs.getInt(1) 就是最大匹配的那个
        if (rs.getInt(i) == 1) {
          //一旦匹配了, 就是最大匹配步骤, 就可以退出了
          maxStep = eventSeqCondition.eventSeqList.size - (i - 1)

          //跳出循环
          loopFlag = false
        }

        i += 1
      }

    }

    maxStep
  }
}
