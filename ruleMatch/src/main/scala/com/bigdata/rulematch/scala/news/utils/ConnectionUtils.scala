package com.bigdata.rulematch.scala.news.utils

import java.sql.DriverManager
import java.time.Duration

import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.protocol.ProtocolVersion
import io.lettuce.core.{ClientOptions, RedisClient, RedisURI}
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

  /**
   * 获取redis的RedisCommands对象
   *
   * @return
   */
  def getRedisCommands() = {
    val redisURI: RedisURI = RedisURI.Builder
      .redis(EventRuleConstant.REDIS_HOST, EventRuleConstant.REDIS_PORT)
      .withPassword(EventRuleConstant.REDIS_PASSWORD.toCharArray())
      .build()

    val redisClient = RedisClient.create(redisURI)
    redisClient.setDefaultTimeout(Duration.ofSeconds(5))

    //注意：lettuce 6.X开始默认的protocolVersion是RESP3,如果redis的版本低于6,
    // 需要修改为RESP2,或者将lettuce降低版本到6以下
    val clientOptions: ClientOptions = ClientOptions.builder()
      .autoReconnect(true)
      //Redis 2 to Redis 5应该使用2, Redis6使用3
      .protocolVersion(ProtocolVersion.RESP2)
      .build()

    redisClient.setOptions(clientOptions)

    val connection: StatefulRedisConnection[String, String] = redisClient.connect

    connection
  }
}
