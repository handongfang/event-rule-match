package com.bigdata.rulematch.scala.router

import java.sql.Connection

import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.bean.rule.{EventCondition, EventSeqCondition, RuleCondition}
import com.bigdata.rulematch.scala.service.impl.{ClickHouseQueryServiceImpl, HBaseQueryServiceImpl}
import com.bigdata.rulematch.scala.utils.{ConnectionUtils, EventRuleCompareUtils}
import org.apache.commons.dbutils.DbUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * 简单查询路由, 用于控制查询流程
 */
class SimpleQueryRouter {
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

    //2, 判断是否满足规则触发条件
    if (EventRuleCompareUtils.eventMatchCondition(event, ruleCondition.triggerEventCondition)) {
      logger.debug(s"满足规则的触发条件: ${ruleCondition.triggerEventCondition}")
      //满足规则的触发条件,才继续进行其他规则条件的匹配

      isMatch = true

      //3, 判断是否满足用户画像条件（hbase）
      val userProfileConditions: Map[String, (String, String)] = ruleCondition.userProfileConditions
      if (userProfileConditions != null && userProfileConditions.size > 0) {
        //只有设置了用户画像类条件,才去匹配
        logger.debug(s"开始匹配用户画像类规则条件: ${userProfileConditions}")

        //从hbase中查询，并判断是否匹配
        isMatch = hBaseQueryService.userProfileConditionIsMatch(event.userId, userProfileConditions)
      } else {
        logger.debug("没有设置用户画像规则类条件")
      }

      if (isMatch) {
        logger.debug("用户画像类规则满足,开始匹配行为次数类条件 ")

        //4, 行为次数类条件  （clickhouse）
        val actionCountConditionList = ruleCondition.actionCountConditionList
        if (actionCountConditionList != null && actionCountConditionList.size > 0) {
          logger.debug(s"开始匹配行为次数类规则条件: ${actionCountConditionList}")

          val actionCountIterator = actionCountConditionList.iterator

          while (actionCountIterator.hasNext && isMatch) {

            val countCondition: EventCondition = actionCountIterator.next()

            //判断是否满足行为次数类的规则条件
            val queryCnt = clickHouseQueryService.queryActionCountCondition(ruleCondition.keyByFields, keyByFiedValue, countCondition)
            //拿查询出来的行为次数与规则中要求的规则次数进行比较
            if (queryCnt >= countCondition.minLimit && queryCnt <= countCondition.maxLimit) {
              //行为次数类条件满足,什么都不做,继续判断下一个次数条件是否满足
            } else {
              isMatch = false
            }
          }

        } else {
          logger.debug("没有设置行为次数类规则条件")
        }

        //5, 行为次序类条件  （clickhouse）
        if (isMatch) {
          logger.debug("行为次数类规则满足,开始匹配行为次序类条件 ")

          val actionSeqConditionList = ruleCondition.actionSeqConditionList

          if (actionSeqConditionList != null && actionSeqConditionList.size > 0) {
            logger.debug(s"开始匹配行为次序类规则条件: ${actionSeqConditionList}")

            val actionSeqIterator = actionSeqConditionList.iterator

            while (actionSeqIterator.hasNext && isMatch) {

              val seqCondition: EventSeqCondition = actionSeqIterator.next()

              //查询最大匹配步骤
              val maxStep = clickHouseQueryService.queryActionSeqCondition(ruleCondition.keyByFields, keyByFiedValue, seqCondition)
              //拿查询出来的最大匹配步骤与规则中要求的次序条件个数进行比较
              if (maxStep >= seqCondition.eventSeqList.size) {
                //查询出来的最大匹配步骤满足次序条件的个数,什么都不做,继续判断下一个序列条件是否满足
              } else {
                isMatch = false
              }
            }

          } else {
            logger.debug("没有设置行为次序类规则条件")
          }

        } else {
          logger.debug("行为次序类规则不满足, 规则全部匹配完毕, 没有命中任何规则 ")
        }

      } else {
        logger.debug("行为次数类规则不满足, 不再进行后续匹配 ")
      }

    } else {
      logger.debug(s"不满足规则的触发条件: ${ruleCondition.triggerEventCondition}")
    }

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
