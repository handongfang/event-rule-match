package com.bigdata.rulematch.java.bean.rule;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  11:27
 */

import java.util.HashMap;
import java.util.Map;

/**
 * 规则条件中，最原子的一个封装，封装“1个”事件条件
 * 要素：
 * 事件id
 * 事件属性约束
 * 事件时间约束
 * 事件次数约束
 */
public class EventCondition {

    /**
     * 初始化
     *
     * @param eventId
     * @param eventProps
     * @param timeRangeStart
     * @param timeRangeEnd
     * @param minLimit
     * @param maxLimit
     */
    public EventCondition(String eventId, Map<String, String> eventProps, Long timeRangeStart, Long timeRangeEnd, int minLimit, int maxLimit) {
        this.eventId = eventId;
        this.eventProps = eventProps;
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
    }

    /**
     * 初始化
     *
     * @param eventId
     * @param eventProps
     * @param timeRangeStart
     * @param timeRangeEnd
     */
    public EventCondition(String eventId, Map<String, String> eventProps, Long timeRangeStart, Long timeRangeEnd) {
        this.eventId = eventId;
        this.eventProps = eventProps;
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
    }

    /**
     * 初始化
     *
     * @param eventId
     * @param eventProps
     */
    public EventCondition(String eventId, Map<String, String> eventProps) {
        this.eventId = eventId;
        this.eventProps = eventProps;
    }

    /**
     * 规则条件中的一个事件的id
     */
    String eventId;
    /**
     * 规则条件中的一个事件的属性约束
     */
    Map<String, String> eventProps = new HashMap<String, String>();
    /**
     * 规则条件中的一个事件要求的发生时间段起始
     */
    Long timeRangeStart = 0L;
    /**
     * 规则条件中的一个事件要求的发生时间段终点
     */
    Long timeRangeEnd = 0L;
    /**
     * 规则条件中的一个事件要求的发生次数最小值
     */
    int minLimit = 0;

    /**
     * 规则条件中的一个事件要求的发生次数最大值
     */
    int maxLimit = 0;


    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Map<String, String> getEventProps() {
        return eventProps;
    }

    public void setEventProps(Map<String, String> eventProps) {
        this.eventProps = eventProps;
    }

    public Long getTimeRangeStart() {
        return timeRangeStart;
    }

    public void setTimeRangeStart(Long timeRangeStart) {
        this.timeRangeStart = timeRangeStart;
    }

    public Long getTimeRangeEnd() {
        return timeRangeEnd;
    }

    public void setTimeRangeEnd(Long timeRangeEnd) {
        this.timeRangeEnd = timeRangeEnd;
    }

    public int getMinLimit() {
        return minLimit;
    }

    public void setMinLimit(int minLimit) {
        this.minLimit = minLimit;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public void setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
    }
}
