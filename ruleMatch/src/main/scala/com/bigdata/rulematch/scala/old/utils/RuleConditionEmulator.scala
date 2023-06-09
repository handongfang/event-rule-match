package com.bigdata.rulematch.scala.old.utils

import com.bigdata.rulematch.scala.old.bean.rule.{EventCondition, EventSeqCondition, RuleCondition}
import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
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
    val userProfileConditions: Map[String, (String, String)] = Map[String, (String, String)](
      "sex" -> (EventRuleConstant.OPERATOR_EQUEAL, "female"),
      "age" -> (EventRuleConstant.OPERATOR_GREATERTHAN, "18")
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

    /**
     * 由于跨界查询的时候,时间并不是取的规则中的时间,所以时间不能在这里拼装
     */
    var actionCountQuerySql =
      s"""
         |SELECT count(1) AS cnt
         | FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND properties['productId'] = 'A'
         | AND eventId = '${EventRuleConstant.EVENT_ADD_CART}'
         | AND `timeStamp` BETWEEN ? AND ?
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
         | AND `timeStamp` BETWEEN ? AND ?
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

    val eventSeqList: List[EventCondition] = List[EventCondition](
      actionSeqCondition1, actionSeqCondition2, actionSeqCondition3
    )

    //次序类查询的sql语句
    /**
     * 由于跨界查询的时候,时间并不是取的规则中的时间,所以时间不能在这里拼装
     */
    val actionSeqQuerySql =
      s"""
         |SELECT
         |    sequenceMatch('.*(?1).*(?2).*(?3).*')(
         |    toDateTime(`timeStamp`),
         |    eventId = '${EventRuleConstant.EVENT_PAGE_VIEW}',
         |    eventId = '${EventRuleConstant.EVENT_ADD_CART}',
         |    eventId = '${EventRuleConstant.EVENT_ORDER_SUBMIT}'
         |   ) AS is_match3,
         |  sequenceMatch('.*(?1).*(?2).*')(
         |    toDateTime(`timeStamp`),
         |    eventId = '${EventRuleConstant.EVENT_PAGE_VIEW}',
         |    eventId = '${EventRuleConstant.EVENT_ADD_CART}',
         |    eventId = '${EventRuleConstant.EVENT_ORDER_SUBMIT}'
         |  ) AS is_match2,
         | sequenceMatch('.*(?1).*')(
         |    toDateTime(`timeStamp`),
         |    eventId = '${EventRuleConstant.EVENT_PAGE_VIEW}',
         |    eventId = '${EventRuleConstant.EVENT_ADD_CART}',
         |    eventId = '${EventRuleConstant.EVENT_ORDER_SUBMIT}'
         |  ) AS is_match1
         |FROM ${EventRuleConstant.CLICKHOUSE_TABLE_NAME}
         |WHERE ${keyByFields} = ? AND `timeStamp` BETWEEN ? AND ?
         |  AND (
         |        (eventId='${EventRuleConstant.EVENT_PAGE_VIEW}' AND properties['pageId']='A')
         |        OR
         |        (eventId = '${EventRuleConstant.EVENT_ADD_CART}' AND properties['productId']='B')
         |        OR
         |        (eventId = '${EventRuleConstant.EVENT_ORDER_SUBMIT}' AND properties['productId']='B')
         |    )
         |GROUP BY userId
         |""".stripMargin

    //行为序列也可以有多个
    val eventSeqCondition1 = EventSeqCondition(actionSeqConditionStartTime, actionSeqConditionEndTime, eventSeqList, actionSeqQuerySql)

    val actionSeqConditionList = List[EventSeqCondition](eventSeqCondition1)

    /**
     * 封装规则条件并返回
     */
    RuleCondition(ruleId, keyByFields, triggerCondition, userProfileConditions,
      actionCountConditionList, actionSeqConditionList)
  }
}
