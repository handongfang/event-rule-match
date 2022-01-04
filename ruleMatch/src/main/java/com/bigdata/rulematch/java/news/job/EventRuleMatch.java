package com.bigdata.rulematch.java.news.job;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.RuleMatchResult;
import com.bigdata.rulematch.java.news.functions.EventJSONToBeanFlatMapFunction;
import com.bigdata.rulematch.java.news.functions.RuleMatchKeyedProcessFunction;
import com.bigdata.rulematch.java.news.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于事件的静态规则匹配Java版本
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 17:55
 */
public class EventRuleMatch {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static String checkpointDataUri = "";
    /**
     * 消费的kafka主题名称
     */
    public static String consumerTopics = "user-event";

    private static boolean isLocal = true;

    public static void main(String[] args) throws Exception {
        //1.创建执行的环境
        /*if(isLocal){
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        }else {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        }*/
        StreamExecutionEnvironment env = isLocal ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()) : StreamExecutionEnvironment.getExecutionEnvironment();

        //为了便于观察,把并行度设置为1
        env.setParallelism(1);

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
        KeyedStream<EventLogBean, String> keyedDS = eventLogBeanDS.keyBy(eventLogBean -> eventLogBean.getUserId());

        keyedDS.process(new RuleMatchKeyedProcessFunction());

        //matchRuleDS.print("matchRuleDS");

        env.execute("EventRuleMatchV1");
    }
}
