package com.bigdata.rulematch.scala.function

import com.bigdata.rulematch.scala.bean.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import com.bigdata.rulematch.scala.datagen.RuleConditionEmulator
import com.bigdata.rulematch.scala.service.HBaseQueryServiceImpl
import com.bigdata.rulematch.scala.utils.{ConnectionUtils, EventRuleCompareUtils}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
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
  private var hbaseConn: Connection = null

  private var hBaseQueryService: HBaseQueryServiceImpl = null

  override def open(parameters: Configuration): Unit = {

    // 初始化hbase连接
    hbaseConn = ConnectionUtils.getHBaseConnection()

    //初始化hbase查询服务对象
    hBaseQueryService = new  HBaseQueryServiceImpl(hbaseConn)
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

      var isMatch = false

      //3, 判断是否满足用户画像条件（hbase）
      val userProfileConditions: Map[String, String] = ruleCondition.userProfileConditions
      if (userProfileConditions != null && userProfileConditions.size > 0) {
        //只有设置了用户画像类条件,才去匹配
        logger.debug(s"开始匹配用户画像类规则条件: ${userProfileConditions}")

        //从hbase中查询，并判断是否匹配
        isMatch = hBaseQueryService.userProfileConditionIsMatch(event.eventId, userProfileConditions)

      } else {
        logger.debug("没有设置用户画像规则类条件")
      }

      //4, TODO 行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）

      //5, TODO 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）

      if (isMatch) {

        //创建规则匹配结果对象
        val matchResult = RuleMatchResult("rule-001", "规则1", event.timeStamp, System.currentTimeMillis())

        //将匹配结果输出
        out.collect(matchResult)
      }
    } else {
      logger.debug(s"不满足规则的触发条件: ${ruleCondition.triggerEventCondition}")
    }

  }

  override def close(): Unit = {
    //关闭hbase连接
    hbaseConn.close()
  }
}
