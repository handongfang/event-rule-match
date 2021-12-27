package com.bigdata.rulematch.scala.old.bean


import com.alibaba.fastjson.JSON

/**
 *
 * JSON字符串解析成 EventLogBean
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 23:10
 */
object BeanTest {
  def main(args: Array[String]): Unit = {
    val jsonStr = "{\"userId\":\"u202112180001\",\"timeStamp\":1639839496220,\"eventId\":\"productView\",\"properties\":{\"pageId\":\"646\",\"productId\":\"157\",\"title\":\"爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码\",\"url\":\"https://item.jd.com/36506691363.html\"}}"

    val eventLogBean: EventLogBean = JSON.parseObject(jsonStr, classOf[EventLogBean])

    println(eventLogBean)
    println(eventLogBean.properties)

    /*val jSONObject = JSON.parseObject(jsonStr)

    val userId = jSONObject.getString("userId")
    val eventId = jSONObject.getString("eventId")
    val timeStamp = jSONObject.getLongValue("timeStamp")

    import scala.collection.JavaConverters._

    val properties: Map[String, String] = jSONObject.getJSONObject("properties").getInnerMap
      .asInstanceOf[Map[String, String]]

    val eventLogBean2 = EventLogBean(userId, eventId, timeStamp, properties)
    println(eventLogBean2)*/
  }
}
