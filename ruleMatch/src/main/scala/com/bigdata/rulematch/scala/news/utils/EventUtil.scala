package com.bigdata.rulematch.scala.news.utils

import java.util.regex.Pattern

import com.bigdata.rulematch.scala.news.beans.EventLogBean
import com.bigdata.rulematch.scala.news.beans.rule.EventCondition
import org.apache.commons.lang3.StringUtils

object EventUtil {

  /**
   * 在传入的事件序列中匹配给定的模式，并返回匹配的次数
   * @param eventStr
   * @param matchPattern
   */
  def sequenceStrMatchRegexCount(eventStr: String, matchPattern: String) = {

    //匹配正则表达式，得到匹配的次数
    val pattern = Pattern.compile(matchPattern)

    val matcher = pattern.matcher(eventStr)

    var matchCount = 0
    while (matcher.find()) {
      matchCount += 1
    }

    matchCount

  }

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
