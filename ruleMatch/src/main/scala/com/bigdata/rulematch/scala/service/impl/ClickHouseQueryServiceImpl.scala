package com.bigdata.rulematch.scala.service.impl

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.bigdata.rulematch.scala.bean.rule.EventCondition
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import org.apache.commons.dbutils.DbUtils
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.server.ZooKeeperServer.DataTreeBuilder
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

  def queryActionCountCondition(keyByField: String ,
                                keyByFieldValue: String ,
                                eventCondition: EventCondition) = {

    logger.debug("CK收到一个行为次数类查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCondition)

    val propertiesConditionBuilder = new StringBuilder

    val eventProps = eventCondition.eventProps

    if(eventProps != null && eventProps.size > 0){
      eventProps.foreach{ case (k, v) => {
        propertiesConditionBuilder.append(s" AND ${k} = '${v}'")
      }}
    }

    val querySqlStr =
      s"""
         |SELECT count(1) AS cnt
         | FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByField} = ? ${propertiesConditionBuilder.toString()}
         | AND eventId = ${eventCondition.eventId} AND timeStamp BETWEEN ? AND ?
         |""".stripMargin

    val pstmt: PreparedStatement = ckConn.prepareStatement(querySqlStr)

    pstmt.setString(1, keyByFieldValue)
    pstmt.setLong(2, eventCondition.timeRangeStart)
    pstmt.setLong(3, eventCondition.timeRangeEnd)


    val rs: ResultSet = pstmt.executeQuery()

    var count = 0L

    while(rs.next()){
      count = rs.getLong("cnt")

      println(s"查询到的行为次数: ${count}")
    }


    //不能关闭连接Connection,因为需要一直查,连接关闭在RuleMatchKeyedProcessFunction的close方法中
    DbUtils.closeQuietly(null, pstmt, rs)

    //返回查询到的结果
    count
  }
}
