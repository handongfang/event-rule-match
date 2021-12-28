package com.bigdata.rulematch.scala.old.source

import java.util.Properties

import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.{KafkaSource, KafkaSourceOptions}
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}
import org.apache.kafka.common.serialization.StringDeserializer

/**
 *
 * 用于创建kafka source的工具类
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 18:03
 */
object KafkaSourceFactory {

  private val config: PropertiesConfiguration = EventRuleConstant.config

  def getKafkaSource(topics: String) = {
    val properties: Properties = new Properties

    val groupId = "g-rule-match"

    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS))
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    //关闭kafka自动提交偏移量，其实如果开启了checkpoint,kafka就不会再自动提交偏移量了，这个参数可以不设置
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    //关闭kafka自动提交偏移量，其实如果开启了checkpoint,kafka就不会再自动提交偏移量了，这个参数可以不设置
    properties.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "true")

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setProperties(properties)
      .setBootstrapServers(config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS))
      .setTopics(topics)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    kafkaSource
  }
}
