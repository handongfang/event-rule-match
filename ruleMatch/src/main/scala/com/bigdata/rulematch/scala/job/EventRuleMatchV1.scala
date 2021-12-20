package com.bigdata.rulematch.scala.job

import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import com.bigdata.rulematch.scala.function.{EventJSONToBeanFlatMapFunction, RuleMatchKeyedProcessFunctionV1}
import com.bigdata.rulematch.scala.source.KafkaSourceFactory
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 基于事件消息的静态规则匹配作业
 *
 * 触发条件: 浏览A页面
 * 用户画像条件： 性别：女; 年龄: >18岁
 * 行为次数类条件：2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
 * 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 17:55
 */
object EventRuleMatchV1 {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val checkpointDataUri = ""
  /**
   * 消费的kafka主题名称
   */
  val consumerTopics = "user-event"

  private val isLocal = true

  def main(args: Array[String]): Unit = {
    //1.创建执行的环境
    val env: StreamExecutionEnvironment = if (isLocal) {
      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    } else {
      StreamExecutionEnvironment.getExecutionEnvironment
    }

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    //创建kaka数据源
    val kafkaSource: KafkaSource[String] = KafkaSourceFactory.getKafkaSource(consumerTopics)

    /**
     * 从kafka中接收事件明细数据，每产生一个事件，都会发送到kafka
     */
    val eventDS: DataStream[String] = env.fromSource(kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "EventKafkaSource")
      .uid("rule-match-20211218001")

    //将JSON转换为 EventLogBean
    val eventLogBeanDS: DataStream[EventLogBean] = eventDS.flatMap(new EventJSONToBeanFlatMapFunction)

    //eventLogBeanDS.print()

    //因为规则匹配是针对每个用户，kyBY后单独继续匹配的
    val keyedDS: KeyedStream[EventLogBean, String] = eventLogBeanDS.keyBy(_.userId)

    val matchRuleDS = keyedDS.process(new RuleMatchKeyedProcessFunctionV1)

    matchRuleDS.print("matchRuleDS")

    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
