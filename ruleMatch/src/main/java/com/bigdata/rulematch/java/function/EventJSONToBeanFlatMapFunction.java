package com.bigdata.rulematch.java.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.rulematch.java.bean.EventLogBean;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HanDongfang
 * @create 2021-12-19  22:02
 */
public class EventJSONToBeanFlatMapFunction extends RichFlatMapFunction<String, EventLogBean> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Override
    public void flatMap(String s, Collector<EventLogBean> collector) throws Exception {
        try {

            JSONObject jsonObject = JSON.parseObject(s);
            /*String userId = jsonObject.getString("userId");
            String eventId = jsonObject.getString("eventId");
            Long timeStamp = jsonObject.getLong("timeStamp");
            String properties = jsonObject.getString("properties");*/
            //将JSON 数据变成 EventLogBean
            EventLogBean eventLogBean = JSON.toJavaObject(jsonObject, EventLogBean.class);

            collector.collect(eventLogBean);

        } catch (Exception e) {
            logger.warn("数据格式出错: " + s);
        }
    }
}
