package com.bigdata.rulematch.scala.router

import java.sql.Connection

import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.bean.rule.{EventCondition, EventSeqCondition, RuleCondition}
import com.bigdata.rulematch.scala.service.impl.{ClickHouseQueryServiceImpl, HBaseQueryServiceImpl}
import com.bigdata.rulematch.scala.utils.{ConnectionUtils, EventRuleCompareUtils, SegmentQueryUtil}
import org.apache.commons.dbutils.DbUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * 支持分段的查询路由, 用于控制分段查询流程
 */
class SegmentQueryRouter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  /**
   * hbase连接
   */
  private val hbaseConn: org.apache.hadoop.hbase.client.Connection = ConnectionUtils.getHBaseConnection()
  /**
   * clickhouse连接
   */
  private val ckConn: Connection = ConnectionUtils.getClickHouseConnection()

  //初始化hbase查询服务对象
  private val hBaseQueryService: HBaseQueryServiceImpl = new HBaseQueryServiceImpl(hbaseConn)

  //初始化clickhouse查询服务对象
  private val clickHouseQueryService: ClickHouseQueryServiceImpl = new ClickHouseQueryServiceImpl(ckConn)

  /**
   * 规则匹配
   */
  def ruleMatch(event: EventLogBean,
                keyByFiedValue:  String,
                ruleCondition: RuleCondition) = {
    var isMatch = false

    //根据当前的查询时间（也可以根据事件中的时间）,获取查询分界点
    val queryBoundPoint: Long = SegmentQueryUtil.getBoundPoint(System.currentTimeMillis())

    //TODO 开始根据查询分界点, 进行分段查询

    isMatch
  }

  /**
   * 关闭连接
   */
  def closeConnection() = {
    //关闭hbase连接
    if (hbaseConn != null) {
      try {
        hbaseConn.close()
      } catch {
        case _ =>
      }
    }

    //关闭clickhouse连接
    DbUtils.closeQuietly(ckConn)
  }
}
