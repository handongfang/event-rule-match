package com.bigdata.rulematch.java.datagen;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.java.job.EventRuleMatchV1;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.SQLException;
import java.util.Properties;

/**
 * 用来生成事件明细测试数据的模拟器
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  12:09
 */
public class EventLogAutoGen {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        String eventLogJSONStr = String.format("{" +
                        "\"userId\":\"u202112180001\"," +
                        "\"timeStamp\": %d ,\"eventId\": %s," +
                        "\"properties\":{\"pageId\":\"646\",\"productId\":\"157\"," +
                        "\"title\":\"爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码\"," +
                        "\"url\":\"https://item.jd.com/36506691363.html\"}" +
                        "}"
                , System.currentTimeMillis(), EventRuleConstant.EVENT_PAGE_VIEW);

        EventLogBean eventLogBean = JSON.parseObject(eventLogJSONStr, EventLogBean.class);

        System.out.println(eventLogBean);

        //发送到kafka
        eventLogDataToKafka(eventLogBean);

        //插入到CK
        ClickHouseDataMock.eventLogDataToClickHouse(eventLogBean);
    }

    public static void eventLogDataToKafka(EventLogBean eventLog) {
        Properties properties = new Properties();

        PropertiesConfiguration config = EventRuleConstant.config;

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(EventRuleConstant.KAFKA_BOOTSTRAP_SERVERS));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String message = JSON.toJSONString(eventLog, SerializerFeature.WRITE_MAP_NULL_FEATURES);

        System.out.println(String.format("发送到kafka的数据: %s", message));

        kafkaProducer.send(new ProducerRecord<String, String>(EventRuleMatchV1.consumerTopics, message));
    }
}
