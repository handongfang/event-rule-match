package com.bigdata.rulematch.scala.news.functions

import com.bigdata.rulematch.scala.news.beans.rule.MatchRule
import com.bigdata.rulematch.scala.news.beans.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.news.utils.{EventUtil, RuleConditionEmulator, StateDescUtils}
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

  override def open(parameters: Configuration): Unit = {

    // 初始化查询路由对象
    //queryRouter = new SimpleQueryRouter()

    //初始化用于存放2小时内事件明细的状态
    eventListState = getRuntimeContext.getListState[EventLogBean](StateDescUtils.getEventBeanStateDesc())
  }

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //将当前收到 event 放入 flink 的state中，state设置的有效期为2小时
    eventListState.add(event)

    //获取模拟生成的规则
    val ruleCondition: MatchRule = RuleConditionEmulator.getRuleConditions()

    logger.debug(s"获取到的规则条件: ${ruleCondition}")

    //判断是否满足规则触发条件
    val triggerEventCondition = ruleCondition.triggerEventCondition
    val isMatch = EventUtil.eventMatchCondition(event, triggerEventCondition)

    //查询用户画像

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
    //queryRouter.closeConnection()
  }
}
