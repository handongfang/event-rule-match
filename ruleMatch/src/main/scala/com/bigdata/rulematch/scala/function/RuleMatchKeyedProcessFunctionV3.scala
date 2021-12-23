package com.bigdata.rulematch.scala.function

import java.sql.Connection

import com.bigdata.rulematch.scala.bean.rule.{EventCondition, EventSeqCondition}
import com.bigdata.rulematch.scala.bean.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import com.bigdata.rulematch.scala.datagen.RuleConditionEmulator
import com.bigdata.rulematch.scala.router.SimpleQueryRouter
import com.bigdata.rulematch.scala.service.impl.{ClickHouseQueryServiceImpl, HBaseQueryServiceImpl}
import com.bigdata.rulematch.scala.utils.{ConnectionUtils, EventRuleCompareUtils}
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.dbutils.DbUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 静态规则匹配KeyedProcessFunction 版本3
 *
 * 主要是把查询的流程放到一个路由器类中进行管理
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:07
 */
class RuleMatchKeyedProcessFunctionV3 extends KeyedProcessFunction[String, EventLogBean, RuleMatchResult] {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val config: PropertiesConfiguration = EventRuleConstant.config

  private var queryRouter: SimpleQueryRouter = null

  override def open(parameters: Configuration): Unit = {

    // 初始化查询路由对象
    queryRouter = new SimpleQueryRouter()
  }

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //获取模拟生成的规则
    val ruleCondition = RuleConditionEmulator.getRuleConditions()

    logger.debug(s"获取到的规则条件: ${ruleCondition}")

    //判断是否满足规则触发条件
    val isMatch = queryRouter.ruleMatch(event, ctx.getCurrentKey, ruleCondition)

    if (isMatch) {

      logger.info("所有规则匹配成功,准备输出匹配结果信息...")

      //创建规则匹配结果对象
      val matchResult = RuleMatchResult(ctx.getCurrentKey, ruleCondition.ruleId, event.timeStamp, System.currentTimeMillis())

      //将匹配结果输出
      out.collect(matchResult)
    }

  }

  override def close(): Unit = {
    //关闭连接
    queryRouter.closeConnection()
  }
}
