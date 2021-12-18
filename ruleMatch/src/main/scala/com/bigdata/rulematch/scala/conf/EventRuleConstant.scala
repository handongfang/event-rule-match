package com.bigdata.rulematch.scala.conf

import org.apache.commons.configuration2.builder.fluent.Configurations

/**
 *
 * 项目中使用到的配置信息或者常量信息
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 18:07
 */
object EventRuleConstant {
  private val configs = new Configurations()

  //加载application.properties配置文件
  val config = configs.properties("application.properties")

  //Kafka相关的配置参数
  val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"

  //ClickHouse相关配置
  val CLICKHOUSE_TABLE_NAME = "default.event_detail"
  val CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver"
  val CLICKHOUSE_URL = "jdbc:clickhouse://82.156.210.70:8123/default"
}
