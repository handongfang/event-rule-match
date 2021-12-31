package com.bigdata.rulematch.scala.news.controller

import com.bigdata.rulematch.scala.news.beans.EventLogBean
import com.bigdata.rulematch.scala.news.beans.rule.MatchRule
import com.bigdata.rulematch.scala.news.service.TriggerModeRuleMatchServiceImpl
import com.bigdata.rulematch.scala.news.utils.EventUtil
import org.apache.flink.api.common.state.ListState
import org.slf4j.{Logger, LoggerFactory}


class TriggerModelRuleMatchController {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var triggerModeRuleMatchService: TriggerModeRuleMatchServiceImpl = null

  def this(eventListState: ListState[EventLogBean]) = {
    this()
    triggerModeRuleMatchService = new TriggerModeRuleMatchServiceImpl(eventListState)
  }


  /**
   * 判断是否满足规则
   *
   * @param ruleCondition
   * @param event
   */
  def ruleIsMatch(ruleCondition: MatchRule, event: EventLogBean) = {
    //判断当前数据bean是否满足规则的触发事件条件
    //判断是否满足规则触发条件
    val triggerEventCondition = ruleCondition.triggerEventCondition
    var isMatch = EventUtil.eventMatchCondition(event, triggerEventCondition)

    if (isMatch) {
      //满足触发条件，再继续判断
      //先判断是否有用户画像条件
      val userProfileConditions = ruleCondition.userProfileConditions
      if (userProfileConditions != null && userProfileConditions.size > 0) {
        //存在用户画像条件
        //开始判断用户画像条件是否满足
        isMatch = triggerModeRuleMatchService.matchProfileCondition(userProfileConditions, event.userId)
        if (!isMatch) {
          logger.debug(s"画像条件不满足, 不再进行后续匹配, userId: ${event.userId}, 画像条件: ${userProfileConditions}")
        }
      }

      //组合条件是在满足触发条件后就可以判断，不能放在画像条件满足后面判断，因为有可能没有画像条件。
      if (isMatch) {
        //获取组合条件，判断是否存在组合条件
        val eventCombinationConditionList = ruleCondition.eventCombinationConditionList

        if (eventCombinationConditionList != null && eventCombinationConditionList.size > 0) {
          //设置了组合条件，开始开始判断组合条件
          val combinationConditionIterator = eventCombinationConditionList.iterator
          while (combinationConditionIterator.hasNext && isMatch) {
            val eventCombinationCondition = combinationConditionIterator.next()
            //只要有一个组合条件不满足,循环就会终止
            isMatch = triggerModeRuleMatchService.matchEventCombinationCondition(event, eventCombinationCondition)
            // 暂时写死（多个组合条件之间的关系是“且”）,  后面会再优化, 多个组合条件之间, 有可能是 或、与、非 等关系
            if (!isMatch) {
              logger.debug(s"循环终止, userId: ${event.userId}, 组合条件不满足: ${eventCombinationCondition}")
            }
          }
        }
      }
    }

    //最后返回是否满足所有规则条件
    isMatch
  }

  /**
   * 关闭程序中使用的各种连接对象
   */
  def closeConnection() = {
    triggerModeRuleMatchService.closeConnection()
  }
}
