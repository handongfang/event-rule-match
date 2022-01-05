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

  private var timerInfoState: ListState[(MatchRule, Long)] = null

  //条件匹配控制器
  private var triggerModelRuleMatchController: TriggerModelRuleMatchController = null

  //规则条件数组（有可能存在多个规则）
  private var ruleConditionArray: Array[MatchRule] = Array.empty[MatchRule]

  override def open(parameters: Configuration): Unit = {

    //初始化用于存放2小时内事件明细的状态
    eventListState = getRuntimeContext.getListState[EventLogBean](StateDescUtils.getEventBeanStateDesc())

    // 记录规则定时信息的state
    timerInfoState = getRuntimeContext.getListState(StateDescUtils.ruleTimerStateDesc)

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
        logger.info("所有规则匹配完毕,准备输出匹配结果信息...")

        val timerConditionList = ruleCondition.timerConditionList
        if(timerConditionList != null && timerConditionList.size > 0){
          //如果是带定时器的规则

          //注册定时器
          // 目前限定一个规则中只有一个时间条件
          val timerCondition = timerConditionList(0)

          val triggerTime = event.timeStamp + timerCondition.timeLate

          ctx.timerService().registerEventTimeTimer(triggerTime)

          // 在 定时信息state中进行记录
          // 不同的规则，比如rule1和rule2都注册了一个10点的定时器,定时器触发的时候,需要知道应该检查哪个规则
          timerInfoState.add((ruleCondition, triggerTime))

        }else{
          //不带定时器，之间输出结果
          //创建规则匹配结果对象
          val matchResult = RuleMatchResult(ctx.getCurrentKey, ruleCondition.ruleId, event.timeStamp, System.currentTimeMillis())

          //将匹配结果输出
          out.collect(matchResult)
        }
      }

    })

  }


  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#OnTimerContext,
                       out: Collector[RuleMatchResult]): Unit = {

    val iterator = timerInfoState.get().iterator

    while(iterator.hasNext){
      val (matchRule, triggerTime) = iterator.next()

      if(triggerTime == timestamp){
        //说明是本次需要判断的定时条件，否则什么都不做
        val timerConditionList = matchRule.timerConditionList

        //因为目前只支持一个定时条件,所以直接取第0个
        val timerCondition = timerConditionList(0)

        val isMatch = triggerModelRuleMatchController.isMatchTimeCondition(ctx.getCurrentKey, timerCondition,
          timestamp - timerCondition.timeLate, timestamp)

        // 清除已经检查完毕的规则定时点state信息
        iterator.remove()

        if(isMatch){
          //创建规则匹配结果对象
          val matchResult = RuleMatchResult(ctx.getCurrentKey, matchRule.ruleId, timestamp, System.currentTimeMillis())

          //将匹配结果输出
          out.collect(matchResult)
        }

      }else if (triggerTime < timestamp) {
        // 增加删除过期定时信息的逻辑 - 双保险（一般情况下不会出现<的记录）
        iterator.remove()
      }

    }

  }

  override def close(): Unit = {
    //关闭连接
    triggerModelRuleMatchController.closeConnection()
  }
}
