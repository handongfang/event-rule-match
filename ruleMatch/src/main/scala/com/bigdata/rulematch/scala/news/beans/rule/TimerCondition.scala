package com.bigdata.rulematch.scala.news.beans.rule

/**
 * 规则定时条件封装对象
 *
 * @param timeLate
 * @param eventCombinationConditionList
 */
case class TimerCondition(
                           /**
                            * 需要延迟多久触发
                            */
                           timeLate: Long,
                           /**
                            * 行为组合规则条件
                            */
                           eventCombinationConditionList: List[EventCombinationCondition]
                         )
