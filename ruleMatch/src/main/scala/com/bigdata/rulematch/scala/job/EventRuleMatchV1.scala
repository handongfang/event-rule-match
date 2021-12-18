package com.bigdata.rulematch.scala.job

import com.bigdata.rulematch.scala.source.KafkaSourceFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 基于事件消息的静态规则匹配作业
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
  private val consumerTopics = ""

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
     * 从kafka中接口事件明细数据，每产生一个事件，都会发送到kafka
     */
    val eventDS: DataStream[String] = env.fromSource(kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "KafkaSource")
      .uid("rule-match-20211218001")

    eventDS.print()

    env.execute(this.getClass.getSimpleName.stripSuffix("$"))
  }
}
