package com.bigdata.rulematch.scala.old.service.impl

import com.bigdata.rulematch.scala.old.bean.EventLogBean
import com.bigdata.rulematch.scala.old.bean.rule.{EventCondition, EventSeqCondition}
import com.bigdata.rulematch.scala.old.utils.EventRuleCompareUtils
import org.apache.flink.api.common.state.ListState

/**
 * flink state查询服务接口实现
 */
class StateQueryServiceImpl {
  /**
   * 用于判断List中的事件是否满足给定的次数类规则
   *
   * queryStartTime和queryEndTime主要是考虑到跨界查询才添加的，而不是直接使用条件中的时间
   *
   * @param eventListState
   * @param countCondition
   * @param queryStartTime
   * @param queryEndTime
   * @return
   */
  def stateQueryEventCount(eventListState: ListState[EventLogBean],
                           countCondition: EventCondition,
                           queryStartTime: Long,
                           queryEndTime: Long) = {
    //用于记录满足规则的事件个数
    var matchCount = 0
    val listStateIterator = eventListState.get().iterator()
    while (listStateIterator.hasNext) {
      val stateEventLogBean = listStateIterator.next()
      //判断遍历到的时间时间是否落在规则中要求的时间范围内
      if (stateEventLogBean.timeStamp >= queryStartTime &&
        stateEventLogBean.timeStamp <= queryEndTime) {

        if (EventRuleCompareUtils.eventMatchCondition(stateEventLogBean, countCondition)) {
          //如果事件与规则匹配,匹配数则 +1
          matchCount += 1
        }

      }

    }

    matchCount
  }

  /**
   * 从flink状态中查询给定的行为次数类规则最大满足到了第几步
   * @param eventListState
   * @param eventSeqList
   * @param queryStartTime
   * @param queryEndTime
   */
  def stateQueryEventSequence(eventListState: ListState[EventLogBean],
                              eventSeqList: List[EventCondition],
                              queryStartTime: Long,
                              queryEndTime: Long) = {

    var maxStep = 0

    val eventListStateIterator = eventListState.get().iterator()

    while(eventListStateIterator.hasNext && maxStep < eventSeqList.size){
      val eventLogBean = eventListStateIterator.next()


      if (eventLogBean.timeStamp >= queryStartTime &&
        eventLogBean.timeStamp <= queryEndTime) {

        //在查询时间范围内才开始判断
        if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, eventSeqList(maxStep))) {
          //如果事件与规则匹配,匹配步骤则 +1
          maxStep += 1
        }

      }

    }

    maxStep
  }
}
