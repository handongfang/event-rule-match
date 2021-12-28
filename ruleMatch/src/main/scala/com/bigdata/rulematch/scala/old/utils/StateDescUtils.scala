package com.bigdata.rulematch.scala.old.utils

import com.bigdata.rulematch.scala.old.bean.EventLogBean
import org.apache.flink.api.common.state.{ListStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
 * 状态描述工具类
 */
object StateDescUtils {

  /**
   * 获取一个近期事件状态描述器
   */
  def getEventBeanStateDesc() = {

    val eventBeanListState = new ListStateDescriptor[EventLogBean]("eventBeanListState",
      createTypeInformation[EventLogBean])

    //设置2小时有效
    val stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build()

    eventBeanListState.enableTimeToLive(stateTtlConfig)

    eventBeanListState
  }
}
