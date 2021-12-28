package com.bigdata.rulematch.scala.news.utils

import com.bigdata.rulematch.scala.news.beans.EventLogBean
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

   /* val ttlConfig = StateTtlConfig
      .newBuilder(Time.seconds(1))
      //默认值：创建state和写入state的时候更新,默认值是 OnCreateAndWrite
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build*/

    //设置2小时有效
    val stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build()

    eventBeanListState.enableTimeToLive(stateTtlConfig)

    eventBeanListState
  }
}
