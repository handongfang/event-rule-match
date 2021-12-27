package com.bigdata.rulematch.scala.old.function

import com.bigdata.rulematch.scala.old.bean.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import com.bigdata.rulematch.scala.old.service.impl.HBaseQueryServiceImpl
import com.bigdata.rulematch.scala.old.utils.ConnectionUtils
import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Connection
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 *
 * 静态规则匹配KeyedProcessFunction
 *
 * 触发条件: 浏览A页面
 * 用户画像条件： 性别：女; 年龄: >18岁
 * 行为次数类条件：2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
 * 行为次序类条件: 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:07
 */
class RuleMatchKeyedProcessFunctionV1 extends KeyedProcessFunction[String, EventLogBean, RuleMatchResult] {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val config: PropertiesConfiguration = EventRuleConstant.config

  /**
   * hbase连接
   */
  private var hbaseConn: Connection = null

  private var hBaseQueryService: HBaseQueryServiceImpl = null

  override def open(parameters: Configuration): Unit = {

    // 初始化hbase连接
    hbaseConn = ConnectionUtils.getHBaseConnection()

    //初始化hbase查询服务对象
    hBaseQueryService = new HBaseQueryServiceImpl(hbaseConn)
  }

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //1, 判断是否满足规则触发条件： 浏览A页面
    //过滤出满足触发条件的事件
    if (StringUtils.equals(EventRuleConstant.EVENT_PAGE_VIEW, event.eventId) &&
      StringUtils.equals("A", event.properties.get("pageId"))) {
      logger.debug(s"满足规则的触发条件: EVENT_PAGE_VIEW=${event.eventId},PAGE =${event.properties.get("pageId")}")

      var isMatch = false

      //满足规则触发条件
      //2,判断是否满足用户画像条件  性别：女; 年龄: >18岁  （hbase）
      val userProfileConditions: Map[String, (String, String)] = Map[String, (String, String)](
        "sex" -> (EventRuleConstant.OPERATOR_EQUEAL, "female"),
        "age" -> (EventRuleConstant.OPERATOR_GREATERTHAN, "18")
      )
      if (userProfileConditions != null && userProfileConditions.size > 0) {
        //只有设置了用户画像类条件,才去匹配
        logger.debug(s"开始匹配用户画像类规则条件: ${userProfileConditions}")

        //从hbase中查询，并判断是否匹配
        isMatch = hBaseQueryService.userProfileConditionIsMatch(event.userId, userProfileConditions)

      } else {
        logger.debug("没有设置用户画像规则类条件")
      }

      //3，行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）

      //4，行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）

      if (isMatch) {

        //创建规则匹配结果对象
        val matchResult = RuleMatchResult("rule-001", "规则1", event.timeStamp, System.currentTimeMillis())

        //将匹配结果输出
        out.collect(matchResult)
      }

    }
  }

  override def close(): Unit = {
    //关闭hbase连接
    hbaseConn.close()
  }
}
