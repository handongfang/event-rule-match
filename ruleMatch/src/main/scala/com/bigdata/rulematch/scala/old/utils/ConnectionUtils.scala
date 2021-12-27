package com.bigdata.rulematch.scala.old.utils

import java.sql.DriverManager

import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * 各种连接对象的工具类
 */
object ConnectionUtils {
  private val config: PropertiesConfiguration = EventRuleConstant.config

  /**
   * 获取HBase的连接对象
   *
   * @return
   */
  def getHBaseConnection() = {
    // 初始化hbase连接
    val hbaseConf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

    hbaseConf.set(EventRuleConstant.HBASE_ZOOKEEPER_QUORU, config.getString(EventRuleConstant.HBASE_ZOOKEEPER_QUORU))

    val connection: org.apache.hadoop.hbase.client.Connection = ConnectionFactory.createConnection(hbaseConf)

    connection
  }

  /**
   * 获取 ClickHouse 连接对象
   */
  def getClickHouseConnection() = {
    Class.forName(EventRuleConstant.CLICKHOUSE_DRIVER_NAME)

    val connection: java.sql.Connection = DriverManager.getConnection(EventRuleConstant.CLICKHOUSE_URL)

    connection
  }
}
