package com.bigdata.rulematch.scala.job

import com.bigdata.rulematch.scala.bean.EventLogBean
import com.bigdata.rulematch.scala.function.{EventJSONToBeanFlatMapFunction, RuleMatchKeyedProcessFunctionV1, RuleMatchKeyedProcessFunctionV2}
import com.bigdata.rulematch.scala.source.KafkaSourceFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 基于事件消息的静态规则匹配作业 版本2
 *
 * 把规则进行封装
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 17:55
 */
object EventRuleMatchV2 {
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

    //为了便于观察,把并行度设置为1
    env.setParallelism(1)

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

    val matchRuleDS = keyedDS.process(new RuleMatchKeyedProcessFunctionV2)

    matchRuleDS.print("matchRuleDS")

    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
