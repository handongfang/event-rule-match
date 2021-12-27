package com.bigdata.rulematch.scala.old.function

import java.sql.Connection

import com.bigdata.rulematch.scala.old.bean.rule.{EventCondition, EventSeqCondition}
import com.bigdata.rulematch.scala.old.bean.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import com.bigdata.rulematch.scala.datagen.RuleConditionEmulator
import com.bigdata.rulematch.scala.old.service.impl.{ClickHouseQueryServiceImpl, HBaseQueryServiceImpl}
import com.bigdata.rulematch.scala.old.utils.{ConnectionUtils, EventRuleCompareUtils}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.dbutils.DbUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 静态规则匹配KeyedProcessFunction 版本2
 *
 * 主要是把规则进行封装, 而不是写死在代码中
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:07
 */
class RuleMatchKeyedProcessFunctionV2 extends KeyedProcessFunction[String, EventLogBean, RuleMatchResult] {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val config: PropertiesConfiguration = EventRuleConstant.config

  /**
   * hbase连接
   */
  private var hbaseConn: org.apache.hadoop.hbase.client.Connection = null
  /**
   * clickhouse连接
   */
  private var ckConn: Connection = null

  private var hBaseQueryService: HBaseQueryServiceImpl = null
  private var clickHouseQueryService: ClickHouseQueryServiceImpl = null

  override def open(parameters: Configuration): Unit = {

    // 初始化hbase连接
    hbaseConn = ConnectionUtils.getHBaseConnection()
    // 初始化clickhouse连接
    ckConn = ConnectionUtils.getClickHouseConnection()

    //初始化hbase查询服务对象
    hBaseQueryService = new HBaseQueryServiceImpl(hbaseConn)

    //初始化clickhouse查询服务对象
    clickHouseQueryService = new ClickHouseQueryServiceImpl(ckConn)
  }

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //1. 获取模拟生成的规则
    val ruleCondition = RuleConditionEmulator.getRuleConditions()

    logger.debug(s"获取到的规则条件: ${ruleCondition}")

    //2, 判断是否满足规则触发条件
    if (EventRuleCompareUtils.eventMatchCondition(event, ruleCondition.triggerEventCondition)) {
      logger.debug(s"满足规则的触发条件: ${ruleCondition.triggerEventCondition}")
      //满足规则的触发条件,才继续进行其他规则条件的匹配

      var isMatch = true

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

          val keyByFiedValue = ctx.getCurrentKey

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

            val keyByFiedValue = ctx.getCurrentKey

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

          if (isMatch) {

            logger.info("所有规则匹配成功,准备输出匹配结果信息...")

            //创建规则匹配结果对象
            val matchResult = RuleMatchResult(ctx.getCurrentKey, ruleCondition.ruleId, event.timeStamp, System.currentTimeMillis())

            //将匹配结果输出
            out.collect(matchResult)
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

  }

  override def close(): Unit = {
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
