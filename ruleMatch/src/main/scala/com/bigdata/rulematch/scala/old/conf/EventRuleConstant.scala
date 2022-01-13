package com.bigdata.rulematch.scala.old.conf

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

  //redis相关配置
  val REDIS_HOST = "192.168.1.250"
  val REDIS_PORT = 6379
  val REDIS_PASSWORD = "SoboT321"

  //HBase相关的配置
  //hbase中用户画像表名称
  val HBASE_USER_PROFILE_TABLE_NAME = "user-profile"
  //连接hbase的ZK地址
  val HBASE_ZOOKEEPER_QUORU = "hbase.zookeeper.quorum"

  //用户事件Id类型
  //浏览页面事件
  val EVENT_PAGE_VIEW = "pageView"
  //浏览商品事件
  val EVENT_PRODUCT_VIEW = "productView"
  //添加购物车事件
  val EVENT_ADD_CART = "addCart"
  //收藏事件
  val EVENT_COLLECT = "collect"
  //商品分享事件
  val EVENT_SHARE = "share"
  //提交订单事件
  val EVENT_ORDER_SUBMIT = "orderSubmit"
  //订单支付事件
  val EVENT_ORDER_PAY = "orderPay"

  //用户画像规则比较符号
  //大于
  val OPERATOR_GREATERTHAN = "gt"
  //大于等于
  val OPERATOR_GREATER_EQUEAL = "ge"
  //小于
  val OPERATOR_LESSTHAN = "lt"
  //小于等于
  val OPERATOR_LESS_EQUEAL = "le"
  //等于
  val OPERATOR_EQUEAL = "eq"
  //不等于
  val OPERATOR_NOT_EQUEAL = "neq"
  //包含
  val OPERATOR_CONTAIN = "contain"
  //不包含
  val OPERATOR_NOT_CONTAIN = "notContain"
}
