package com.bigdata.rulematch.java.source;

import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.io.IOException;
import java.util.Properties;

/**
 * @author HanDongfang
 * @create 2021-12-19  17:09
 */
public class KafkaSourceFactory  {

    public static KafkaSource getKafkaSource(String topics) throws IOException, ConfigurationException {

        Properties properties = new Properties();

        String groupId = "g-rule-match";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,EventRuleConstant.config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //关闭kafka自动提交偏移量，其实如果开启了checkpoint,kafka就不会再自动提交偏移量了，这个参数可以不设置
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //关闭kafka自动提交偏移量，其实如果开启了checkpoint,kafka就不会再自动提交偏移量了，这个参数可以不设置
        properties.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "true");

        DeserializationSchema simpleStringSchema = new SimpleStringSchema();
        KafkaSource kafkaSource = KafkaSource.builder().setProperties(properties)
                .setBootstrapServers( EventRuleConstant.config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS))
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(simpleStringSchema)
                .build();

       return kafkaSource;
    }
}
