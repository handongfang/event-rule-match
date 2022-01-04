package com.bigdata.rulematch.scala.news.utils

import com.bigdata.rulematch.scala.news.beans.rule.{EventCombinationCondition, EventCondition, MatchRule}
import com.bigdata.rulematch.scala.news.conf.EventRuleConstant
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
   * 获取多个规则
   * @return
   */
  def getRuleConditionArray() = {
    val ruleCondition: MatchRule = getRuleConditions

    Array[MatchRule](ruleCondition)
  }

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
    val userProfileConditions: Map[String, (String, String)] = Map[String, (String, String)](
      "sex" -> (EventRuleConstant.OPERATOR_EQUEAL, "female"),
      "age" -> (EventRuleConstant.OPERATOR_GREATERTHAN, "18")
    )

    /**
     * 单个行为次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次
     */
    val actionCountCondition1Map: Map[String, String] = Map[String, String](
      "productId" -> "A"
    )
    val actionCountConditionStartTimeStr = "2021-12-10 00:00:00"
    val actionCountConditionEndTimeStr = "9999-12-31 00:00:00"
    val actionCountConditionStartTime = DateUtils.parseDate(actionCountConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime
    val actionCountConditionEndTime = DateUtils.parseDate(actionCountConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime

    /**
     * 用于过滤的sql语句
     */
    var actionCountQuerySql =
      s"""
         |SELECT eventId
         | FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND properties['productId'] = 'A'
         | AND eventId = '${EventRuleConstant.EVENT_ADD_CART}'
         | AND `timeStamp` BETWEEN ? AND ?
         | ORDER BY `timeStamp`
         |""".stripMargin

    var matchPattern = "(1)"

    val actionCountCondition1: EventCondition = EventCondition(EventRuleConstant.EVENT_ADD_CART,
      actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 1,
      Int.MaxValue)

    //构建事件组合条件
    val combinationCondition1 = EventCombinationCondition(actionCountConditionStartTime, actionCountConditionEndTime, 3, Int.MaxValue,
      List[EventCondition](actionCountCondition1), matchPattern, "ck", actionCountQuerySql, "1001")

    //行为组合条件 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
    val actionSeqConditionStartTimeStr = "2021-12-18 00:00:00"
    val actionSeqConditionEndTimeStr = "9999-12-31 00:00:00"

    val actionSeqConditionStartTime = DateUtils.parseDate(actionSeqConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime
    val actionSeqConditionEndTime = DateUtils.parseDate(actionSeqConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime

    val eventMap1: Map[String, String] = Map[String, String](
      "pageId" -> "A"
    )
    val eventCondition1 = EventCondition(EventRuleConstant.EVENT_PAGE_VIEW, eventMap1, actionSeqConditionStartTime, actionSeqConditionEndTime,
      1, Int.MaxValue)

    val eventMap2: Map[String, String] = Map[String, String](
      "productId" -> "B"
    )
    val eventCondition2 = EventCondition(EventRuleConstant.EVENT_ADD_CART, eventMap2, actionSeqConditionStartTime, actionSeqConditionEndTime,
      1, Int.MaxValue)

    val eventMap3: Map[String, String] = Map[String, String](
      "productId" -> "B"
    )
    val eventCondition3 = EventCondition(EventRuleConstant.EVENT_ORDER_SUBMIT, eventMap3, actionSeqConditionStartTime, actionSeqConditionEndTime,
      1, Int.MaxValue)

    //用于过滤的SQL语句
    val actionSeqQuerySql =
      s"""
         |SELECT eventId
         |FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND `timeStamp` BETWEEN ? AND ?
         |  AND (
         |        (eventId='${EventRuleConstant.EVENT_PAGE_VIEW}' AND properties['pageId']='A')
         |        OR
         |        (eventId = '${EventRuleConstant.EVENT_ADD_CART}' AND properties['productId']='B')
         |        OR
         |        (eventId = '${EventRuleConstant.EVENT_ORDER_SUBMIT}' AND properties['productId']='B')
         |    )
         | ORDER BY `timeStamp`
         |""".stripMargin

    //用于匹配的模式
    matchPattern = "(1.*2.*3)"

    val combinationCondition2 = EventCombinationCondition(actionCountConditionStartTime, actionCountConditionEndTime, 1, Int.MaxValue,
      List[EventCondition](eventCondition1, eventCondition2, eventCondition3), matchPattern, "ck", actionSeqQuerySql, "1002")

    /**
     * 封装规则条件并返回
     */
    MatchRule(ruleId, keyByFields, triggerCondition, userProfileConditions,
      List[EventCombinationCondition](combinationCondition1, combinationCondition2))
  }
}
