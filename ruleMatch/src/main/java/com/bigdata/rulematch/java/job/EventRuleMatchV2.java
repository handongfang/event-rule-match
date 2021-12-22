package com.bigdata.rulematch.java.job;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.RuleMatchResult;
import com.bigdata.rulematch.java.function.EventJSONToBeanFlatMapFunction;
import com.bigdata.rulematch.java.function.RuleMatchKeyedProcessFunctionV2;
import com.bigdata.rulematch.java.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于规则的封装处理
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-22  12:20
 */
public class EventRuleMatchV2 {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static String checkpointDataUri = "";
    /**
     * 消费的kafka主题名称
     */
    public static String consumerTopics = "user-event";

    private static boolean isLocal = true;

    public static void main(String[] args) throws Exception {
        //1.创建执行的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        ParameterTool params = ParameterTool.fromArgs(args);

        //创建kaka数据源
        KafkaSource kafkaSource = KafkaSourceFactory.getKafkaSource(consumerTopics);

        /**
         * 从kafka中接收事件明细数据，每产生一个事件，都会发送到kafka
         */
        DataStream<String> eventDS = env.fromSource(kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "EventKafkaSource")
                .uid("rule-match-20211218001");

        //将JSON转换为 EventLogBean
        DataStream<EventLogBean> eventLogBeanDS = eventDS.flatMap(new EventJSONToBeanFlatMapFunction());

        //eventLogBeanDS.print()

        //因为规则匹配是针对每个用户，kyBY后单独继续匹配的
        KeyedStream<EventLogBean, String> keyedDS = eventLogBeanDS.keyBy(bean -> bean.getUserId());

        DataStream<RuleMatchResult> matchRuleDS = keyedDS.process(new RuleMatchKeyedProcessFunctionV2());

        matchRuleDS.print("matchRuleDS");

        env.execute("EventRuleMatchV1");
    }
}
