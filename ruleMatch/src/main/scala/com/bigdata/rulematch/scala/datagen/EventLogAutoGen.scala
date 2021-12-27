package com.bigdata.rulematch.scala.datagen

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.bigdata.rulematch.java.conf.EventRuleConstant
import com.bigdata.rulematch.scala.old.bean.EventLogBean
import com.bigdata.rulematch.scala.old.job.EventRuleMatchV1
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 *
 * 用来生成事件明细测试数据的模拟器
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-20 19:13
 */
object EventLogAutoGen {

  def main(args: Array[String]): Unit = {
    val eventLogJSONStr =
      s"""
         |{
         |	"userId":"u202112180001",
         |	"timeStamp":${System.currentTimeMillis()},
         |	"eventId":"${EventRuleConstant.EVENT_PAGE_VIEW}",
         |	"properties":{
         |		"pageId":"646",
         |		"productId":"157",
         |		"title":"爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码",
         |		"url":"https://item.jd.com/36506691363.html"
         |	}
         |}
         |""".stripMargin

    val eventLogBean: EventLogBean = JSON.parseObject(eventLogJSONStr, classOf[EventLogBean])

    println(eventLogBean)

    //发送到kafka
    eventLogDataToKafka(eventLogBean)

    //插入到CK
    ClickHouseDataMock.eventLogDataToClickHouse(eventLogBean)
  }

  /**
   * EventLog数据发送到Kafka
   */
  def eventLogDataToKafka(eventLog: EventLogBean) = {
    val properties = new Properties()

    val config = EventRuleConstant.config

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS))
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val kafkaProducer = new KafkaProducer[String, String](properties)

    val message = JSON.toJSONString(eventLog, SerializerFeature.WRITE_MAP_NULL_FEATURES)

    println(s"发送到kafka的数据: ${message}")

    kafkaProducer.send(new ProducerRecord[String, String](EventRuleMatchV1.consumerTopics, message))
  }
}
