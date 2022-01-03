package com.bigdata.rulematch.scala.news.service

import com.bigdata.rulematch.scala.news.beans.EventLogBean
import com.bigdata.rulematch.scala.news.beans.rule.EventCombinationCondition
import com.bigdata.rulematch.scala.news.dao.{ClickHouseQuerier, HbaseQuerier, StateQuerier}
import com.bigdata.rulematch.scala.news.utils.{ConnectionUtils, CrossTimeQueryUtil, EventUtil}
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.common.state.ListState
import org.slf4j.{Logger, LoggerFactory}

/**
 * 用于规则匹配的服务类
 */
class TriggerModeRuleMatchServiceImpl {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var stateQuerier: StateQuerier = null
  private var hbaseQuerier: HbaseQuerier = null
  private var clickHouseQuerier: ClickHouseQuerier = null

  def this(eventListState: ListState[EventLogBean]) = {
    this()

    this.stateQuerier = new StateQuerier(eventListState)

    hbaseQuerier = new HbaseQuerier(ConnectionUtils.getHBaseConnection())

    clickHouseQuerier = new ClickHouseQuerier(ConnectionUtils.getClickHouseConnection())
  }

  /**
   * 画像条件匹配
   * @param userProfileConditions
   * @param userId
   */
  def matchProfileCondition(userProfileConditions: Map[String, (String, String)], userId: String) = {

    hbaseQuerier.queryProfileConditionIsMatch(userId, userProfileConditions)

  }


  /**
   * 判断是否满足一个行为组合条件
   * @param event
   * @param combinationCondition
   */
  def matchEventCombinationCondition(event : EventLogBean, combinationCondition: EventCombinationCondition) = {
    //获取查询的分界时间点,如果使用事件时间,如果数据发生延迟,比如10点收到了6点10分的数据,那么查询分界点是5点,5点后的数据都查state,肯定查不到
    val boundPointTime: Long = CrossTimeQueryUtil.getBoundPoint(System.currentTimeMillis())

    // 判断规则条件的时间区间是否跨分界点
    val conditionStart = combinationCondition.timeRangeStart
    val conditionEnd = combinationCondition.timeRangeEnd

    logger.debug(
      s"""
         |分界时间点: ${DateFormatUtils.format(boundPointTime, "yyyy-MM-dd HH:mm:ss")},
         |组合条件要求的起始时间点： ${DateFormatUtils.format(conditionStart, "yyyy-MM-dd HH:mm:ss")},
         |组合条件要求的起始时间点： ${DateFormatUtils.format(conditionEnd, "yyyy-MM-dd HH:mm:ss")}
         |""".stripMargin)

    var isMatch = false

    if(conditionStart >= boundPointTime){
      //                  |------|
      // --------------|--------------
      logger.debug(s"事件Id : ${event.eventId}, 只查询state")
      // 查state状态
      val matchCount = stateQuerier.queryEventCombinationConditionCount("userId", event.userId, combinationCondition,
        conditionStart, conditionEnd)

      if(matchCount >= combinationCondition.minLimit && matchCount <= combinationCondition.maxLimit){
        isMatch = true
      }

    }else if(conditionEnd < boundPointTime){
      logger.debug(s"事件Id : ${event.eventId}, 查询clickHouse")
      //       |------|
      // --------------|--------------
      //查询clickhouse
      val matchCount = clickHouseQuerier.queryEventCombinationConditionCount("userId", event.userId, combinationCondition,
        conditionStart, conditionEnd)

      if(matchCount >= combinationCondition.minLimit && matchCount <= combinationCondition.maxLimit){
        isMatch = true
      }

    }else{
      logger.debug(s"事件Id : ${event.eventId}, 跨区间查询")
      //       |-------------|
      // --------------|--------------
      //跨区间查询

      //先查state状态，看是否能提前结束
      val stateMatchCount = stateQuerier.queryEventCombinationConditionCount("userId", event.userId, combinationCondition,
        boundPointTime, conditionEnd)

      if(stateMatchCount >= combinationCondition.minLimit && stateMatchCount <= combinationCondition.maxLimit){
        isMatch = true
        logger.debug(s"事件Id : ${event.eventId}, 跨区间查询, state中已经满足条件, 规则匹配提前结束")
      }

      if(!isMatch){
        //如果只查state没有满足，再继续查询
        logger.debug(s"事件Id : ${event.eventId}, 跨区间查询, 只查询state中不满足条件, 继续进行规则匹配")

        //查询ck中满足条件的事件序列
        val ckEventSeqStr = clickHouseQuerier.getEventCombinationConditionStr("userId", event.userId, combinationCondition,
          conditionStart, boundPointTime)

        //查询state中的事件序列
        val stateEventSeqStr = stateQuerier.getEventCombinationConditionStr("userId", event.userId, combinationCondition,
          boundPointTime, conditionEnd)

        //拼接事件序列
        val totalMatchCount = EventUtil.sequenceStrMatchRegexCount(ckEventSeqStr + stateEventSeqStr, combinationCondition.matchPattern)

        if(totalMatchCount >= combinationCondition.minLimit && totalMatchCount <= combinationCondition.maxLimit){
          isMatch = true
        }
      }

    }

    isMatch
  }

  /**
   * 关闭程序中使用的各种连接对象
   */
  def closeConnection() = {
    hbaseQuerier.closeConnection()
    clickHouseQuerier.closeConnection()
  }
}
