package com.bigdata.rulematch.scala.news.dao

import java.lang

import com.bigdata.rulematch.scala.news.beans.EventLogBean
import com.bigdata.rulematch.scala.news.beans.rule.EventCombinationCondition
import com.bigdata.rulematch.scala.news.utils.EventUtil
import org.apache.flink.api.common.state.ListState
import org.slf4j.{Logger, LoggerFactory}

/**
 * Flink State状态查询器
 */
class StateQuerier {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var eventListState: ListState[EventLogBean] = null

  def this(eventListState: ListState[EventLogBean]) {
    this()

    this.eventListState = eventListState
  }

  /**
   * 在state中，根据组合条件及查询的时间范围，得到返回结果的1213形式字符串序列
   *
   * queryRangeStart 和 queryRangeEnd这两个参数主要是为分段查询设计的，所以没有直接使用条件中的起止时间
   *
   * @param keyByField                keyby的时候使用的字段名称
   * @param keyByFieldValue           keyby的时候使用的字段对应的值
   * @param eventCombinationCondition 行为组合条件
   * @param queryRangeStart           查询时间范围起始
   * @param queryRangeEnd             查询时间范围结束
   */
  private def getEventCombinationConditionStr(keyByField: String,
                                              keyByFieldValue: String,
                                              eventCombinationCondition: EventCombinationCondition,
                                              queryRangeStart: Long,
                                              queryRangeEnd: Long) = {

    // 获取状态state中的数据迭代器
    val eventBeans: lang.Iterable[EventLogBean] = eventListState.get

    // 获取事件组合条件中的感兴趣的事件
    val eventConditionList = eventCombinationCondition.eventConditionList

    // 迭代state中的数据
    val sb = new StringBuilder()

    val eventBeanIterator = eventBeans.iterator()

    while (eventBeanIterator.hasNext) {
      val eventBean = eventBeanIterator.next()

      if (eventBean.getTimeStamp() >= queryRangeStart
        && eventBean.getTimeStamp() <= queryRangeEnd) {

        var i = 0

        var continueFlag = true

        while ((i < eventConditionList.size) && continueFlag) {

          if (EventUtil.eventMatchCondition(eventBean, eventConditionList(i))) {
            //一旦一个事件匹配上了规则中的一个事件，则跳出当前循环
            sb.append(i + 1)

            continueFlag = true
          }

          i += 1
        }

      }
    }

    sb.toString()
  }

  /**
   * 利用正则表达式查询组合条件满足的次数
   */
  def queryEventCombinationConditionCount(keyByField: String,
                                          keyByFieldValue: String,
                                          eventCombinationCondition: EventCombinationCondition,
                                          queryRangeStart: Long,
                                          queryRangeEnd: Long) = {

    val sequenceStr = getEventCombinationConditionStr(keyByField, keyByFieldValue, eventCombinationCondition, queryRangeStart, queryRangeEnd)

    val matchCount = EventUtil.sequenceStrMatchRegexCount(sequenceStr, eventCombinationCondition.matchPattern)

    matchCount
  }
}
