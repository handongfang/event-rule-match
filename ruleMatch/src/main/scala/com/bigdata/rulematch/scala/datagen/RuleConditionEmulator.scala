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
   */
  def getRuleConditions() = {

    val ruleId = "rule_001"
    val keyByFields = "userId"

    /**
     * 触发事件条件
     */
    val eventProps: Map[String, String] = Map[String, String](
      "pageId" -> "A"
    )
    val triggerCondition = EventCondition(EventRuleConstant.EVENT_PAGE_VIEW, eventProps)

    /**
     * 用户画像条件
     */
    val userProfileConditions: Map[String, String] = Map[String, String](
      "sex" -> "female",
      "ageStart" -> "18",
      "ageEnd" -> "30"
    )

    /**
     * 行为次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
     */
    val actionCountCondition1Map: Map[String, String] = Map[String, String](
      "productId" -> "A"
    )
    val actionCountConditionStartTimeStr = "2021-12-10 00:00:00"
    val actionCountConditionEndTimeStr = "9999-12-31 00:00:00"
    val actionCountConditionStartTime = DateUtils.parseDate(actionCountConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime
    val actionCountConditionEndTime = DateUtils.parseDate(actionCountConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime

    var actionCountQuerySql =
      s"""
         |SELECT count(1) AS cnt
         | FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND properties['productId'] = 'A'
         | AND eventId = '${EventRuleConstant.EVENT_ADD_CART}'
         | AND timeStamp BETWEEN ${actionCountConditionStartTime} AND ${actionCountConditionEndTime}
         |""".stripMargin

    val actionCountCondition1: EventCondition = EventCondition(EventRuleConstant.EVENT_ADD_CART,
      actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 3,
      Int.MaxValue, actionCountQuerySql)


    val actionCountCondition2Map: Map[String, String] = Map[String, String](
      "productId" -> "A"
    )

    actionCountQuerySql =
      s"""
         |SELECT count(1) AS cnt
         | FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND properties['productId'] = 'A'
         | AND eventId = '${EventRuleConstant.EVENT_COLLECT}'
         | AND timeStamp BETWEEN ${actionCountConditionStartTime} AND ${actionCountConditionEndTime}
         |""".stripMargin

    val actionCountCondition2: EventCondition = EventCondition(EventRuleConstant.EVENT_COLLECT,
      actionCountCondition2Map, actionCountConditionStartTime, actionCountConditionEndTime, 5,
      Int.MaxValue, actionCountQuerySql)

    val actionCountConditionList: List[EventCondition] = List[EventCondition](actionCountCondition1, actionCountCondition2)

    /**
     * 行为次序类条件列表 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
     */

    val actionSeqConditionStartTimeStr = "2021-12-18 00:00:00"
    val actionSeqConditionEndTimeStr = "9999-12-31 00:00:00"

    val actionSeqConditionStartTime = DateUtils.parseDate(actionSeqConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime
    val actionSeqConditionEndTime = DateUtils.parseDate(actionSeqConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime

    val actionSeqCondition1Map: Map[String, String] = Map[String, String](
      "pageId" -> "A"
    )
    val actionSeqCondition1: EventCondition = EventCondition(EventRuleConstant.EVENT_PAGE_VIEW,
      actionSeqCondition1Map, actionSeqConditionStartTime, actionSeqConditionEndTime)

    val actionSeqCondition2Map: Map[String, String] = Map[String, String](
      "productId" -> "B"
    )
    val actionSeqCondition2: EventCondition = EventCondition(EventRuleConstant.EVENT_ADD_CART,
      actionSeqCondition2Map, actionSeqConditionStartTime, actionSeqConditionEndTime)

    val actionSeqCondition3Map: Map[String, String] = Map[String, String](
      "productId" -> "B"
    )
    val actionSeqCondition3: EventCondition = EventCondition(EventRuleConstant.EVENT_ORDER_SUBMIT,
      actionSeqCondition3Map, actionSeqConditionStartTime, actionSeqConditionEndTime)

    val actionSeqConditionList: List[EventCondition] = List[EventCondition](
      actionSeqCondition1, actionSeqCondition2, actionSeqCondition3
    )

    /**
     * 封装规则条件并返回
     */
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
