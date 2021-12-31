package com.bigdata.rulematch.scala.news.functions

import com.bigdata.rulematch.scala.news.beans.rule.MatchRule
import com.bigdata.rulematch.scala.news.beans.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.news.controller.TriggerModelRuleMatchController
import com.bigdata.rulematch.scala.news.utils.{RuleConditionEmulator, StateDescUtils}
import org.apache.flink.api.common.state.ListState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 静态规则匹配KeyedProcessFunction 版本4
 *
 * 引入flink状态，根据查询时间的不同，进行分段查询
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:07
 */
class RuleMatchKeyedProcessFunction extends KeyedProcessFunction[String, EventLogBean, RuleMatchResult] {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  //private var queryRouter: SimpleQueryRouter = null

  private var eventListState: ListState[EventLogBean] = null

  //条件匹配控制器
  private var triggerModelRuleMatchController: TriggerModelRuleMatchController = null

  //规则条件数组（有可能存在多个规则）
  private var ruleConditionArray: Array[MatchRule] = Array.empty[MatchRule]

  override def open(parameters: Configuration): Unit = {

    //初始化用于存放2小时内事件明细的状态
    eventListState = getRuntimeContext.getListState[EventLogBean](StateDescUtils.getEventBeanStateDesc())

    //初始化规则匹配控制器
    triggerModelRuleMatchController = new TriggerModelRuleMatchController(eventListState)

    //获取模拟规则
    ruleConditionArray = RuleConditionEmulator.getRuleConditionArray()
  }

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //将当前收到 event 放入 flink 的state中，state设置的有效期为2小时
    eventListState.add(event)

    //遍历所有规则，一个一个去匹配
    ruleConditionArray.foreach(ruleCondition => {
      logger.debug(s"获取到的规则条件: ${ruleCondition}")
      //判断是否满足规条件
      val isMatch = triggerModelRuleMatchController.ruleIsMatch(ruleCondition, event)

      if (isMatch) {

        logger.info("所有规则匹配成功,准备输出匹配结果信息...")

        //创建规则匹配结果对象
        val matchResult = RuleMatchResult(ctx.getCurrentKey, ruleCondition.ruleId, event.timeStamp, System.currentTimeMillis())

        //将匹配结果输出
        out.collect(matchResult)
      }

    })

  }

  override def close(): Unit = {
    //关闭连接
    triggerModelRuleMatchController.closeConnection()
  }
}
