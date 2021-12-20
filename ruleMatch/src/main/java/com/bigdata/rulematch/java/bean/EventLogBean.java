package com.bigdata.rulematch.java.bean;

import scala.beans.BeanProperty;

import java.util.Map;

/**
 * @author HanDongfang
 * @create 2021-12-19  22:03
 */
public class EventLogBean {
    /**
     * 用户Id
     */
    private String userId;

    /**
     * 事件Id
     */
    private String eventId;

    /**
     * 事件发生的时间戳,13位,精确到毫秒
     */
    private Long timeStamp;

    /**
     * 事件的属性, 因为fastjson对Scala的map支持不好,所以这里使用java.util.Map
     */
    private Map<String, String> properties;

    /**
     * 初始化bean
     *
     * @param userId
     * @param eventId
     * @param timeStamp
     * @param properties
     */
    public EventLogBean(String userId, String eventId, Long timeStamp, Map<String, String> properties) {
        this.userId = userId;
        this.eventId = eventId;
        this.timeStamp = timeStamp;
        this.properties = properties;
    }


    public  String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
