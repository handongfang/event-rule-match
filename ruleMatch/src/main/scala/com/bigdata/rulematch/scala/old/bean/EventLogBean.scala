package com.bigdata.rulematch.scala.old.bean

import scala.beans.BeanProperty

/**
 *
 * 封装用户事件明细的Bean对象
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 19:24
 */
case class EventLogBean(
                         /**
                          * 用户Id
                          */
                         @BeanProperty userId: String,

                         /**
                          * 事件Id
                          */
                         @BeanProperty eventId: String,

                         /**
                          * 事件发生的时间戳,13位,精确到毫秒
                          */
                         @BeanProperty timeStamp: Long,

                         /**
                          * 事件的属性, 因为fastjson对Scala的map支持不好,所以这里使用java.util.Map
                          */
                         @BeanProperty properties: java.util.Map[String, String]
                       )
