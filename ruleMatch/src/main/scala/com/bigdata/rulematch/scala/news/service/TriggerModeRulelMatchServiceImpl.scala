package com.bigdata.rulematch.scala.news.service

import com.bigdata.rulematch.scala.news.beans.EventLogBean
import com.bigdata.rulematch.scala.news.beans.rule.EventCombinationCondition
import com.bigdata.rulematch.scala.news.utils.CrossTimeQueryUtil
import org.slf4j.{Logger, LoggerFactory}

/**
 * 用于规则匹配的服务类
 */
object TriggerModeRulelMatchServiceImpl {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))


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

    if(conditionStart >= boundPointTime){
      //                  |------|
      // --------------|--------------
      // 查state状态

    }else if(conditionEnd < boundPointTime){
      //       |------|
      // --------------|--------------
      //查询clickhouse
    }else{
      //       |-------------|
      // --------------|--------------
      //跨区间查询

    }
  }
}
