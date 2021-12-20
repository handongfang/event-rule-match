package com.bigdata.rulematch.scala.utils

import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.bean.rule.EventCondition
import org.apache.commons.lang3.StringUtils

/**
 * 事件规则比较的工具类
 */
object EventRuleCompareUtils {


  /**
   * 比较给定的事件是否与事件原子条件匹配，匹配触发条件的时候,会使用这个方法
   *
   * @param event
   * @param eventCondition
   */
  def eventMatchCondition(event: EventLogBean, eventCondition: EventCondition) = {

    var isMatch = false
    if (StringUtils.equals(event.eventId, eventCondition.eventId)) {
      isMatch = true
      //事件ID相同，还需要判断事件属性是否满足
      val keysIterator = eventCondition.eventProps.keysIterator
      while (keysIterator.hasNext && isMatch) {
        val key = keysIterator.next()
        //规则条件中这个key对应的value
        val conditionValue = eventCondition.eventProps.getOrElse(key, "")

        if (!StringUtils.equals(conditionValue, event.properties.get(key))) {
          //如果事件中某个属性与规则条件中相同key对应的属性值不同,则说明不匹配,需要跳出
          isMatch = false
        }
      }

    }

    //返回是否匹配
    isMatch
  }
}
