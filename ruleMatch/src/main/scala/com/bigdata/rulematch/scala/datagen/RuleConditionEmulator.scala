package com.bigdata.rulematch.scala.datagen

import com.bigdata.rulematch.scala.bean.rule.{EventCombinationCondition, EventCondition, RuleCondition, RuleConditionV2}
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import org.apache.commons.lang3.time.DateUtils

/**
 *
 * 规则条件模拟
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:26
 */
object RuleConditionEmulator {

  /**
   * 获取一个规则
   *
   * @return
   */
  def getRuleConditions() = {

    val ruleId = "rule_001"
    val keyByFields = "userId"

    // 触发事件条件
    //触发事件的属性条件
    val eventProps: Map[String, String] = Map[String, String](
      "pageId" -> "A"
    )
    val triggerCondition = EventCondition(EventRuleConstant.EVENT_PAGE_VIEW, eventProps)

    //用户画像条件
    val userProfileConditions: Map[String, String] = Map[String, String](
      "sex" -> "female",
      "ageStart" -> "18",
      "ageEnd" -> "30"
    )

    // 行为次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
    val actionCountCondition1Map: Map[String, String] = Map[String, String](
      "productId" -> "A"
    )
    val actionCountConditionStartTimeStr = "2021-12-10 00:00:00"
    val actionCountConditionEndTimeStr = "9999-12-31 00:00:00"
    val actionCountConditionStartTime = DateUtils.parseDate(actionCountConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime
    val actionCountConditionEndTime = DateUtils.parseDate(actionCountConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime

    val actionCountCondition1: EventCondition = EventCondition(EventRuleConstant.EVENT_ADD_CART,
      actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 3, Int.MaxValue)

    val actionCountCondition2Map: Map[String, String] = Map[String, String](
      "productId" -> "A"
    )
    val actionCountCondition2: EventCondition = EventCondition(EventRuleConstant.EVENT_COLLECT,
      actionCountCondition2Map, actionCountConditionStartTime, actionCountConditionEndTime, 5, Int.MaxValue)

    val actionCountConditionList: List[EventCondition] = List[EventCondition](actionCountCondition1, actionCountCondition2)

    //行为序列类条件
    val actionSeqConditionList: List[EventCondition] = null

    //行为次序类条件列表

    RuleCondition(ruleId, keyByFields, triggerCondition, userProfileConditions,
      actionCountConditionList, actionSeqConditionList)
  }

  def getRuleConditionsV2(): Unit = {
    val ruleId = "rule_001"
    val keyByFields = "userId"

    // 触发事件条件
    //触发事件的属性条件
    val eventProps: Map[String, String] = Map[String, String](
      "pageId" -> "A"
    )
    val triggerCondition = EventCondition(EventRuleConstant.EVENT_PAGE_VIEW, eventProps)

    //用户画像条件
    val userProfileConditions: Map[String, String] = Map[String, String](
      "sex" -> "female",
      "ageStart" -> "18",
      "ageEnd" -> "30"
    )

    // 行为次数条件列表


    /*val actionCountQuerySql =
      s"""
         |SELECT eventId
         |  FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE eventId = '${EventRuleConstant.EVENT_ADD_CART}'
         |  AND properties['productId']='A'
         |  AND userId = ? AND timeStamp BETWEEN ? AND ?
         |""".stripMargin
    EventCombinationCondition(
      0L, 0L, 3, null, "ck", actionCountQuerySql
    )*/

    //RuleCondition(ruleId, keyByFields, eventCondition, userProfileConditions, )
  }
}
