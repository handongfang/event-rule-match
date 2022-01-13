package com.bigdata.rulematch.java.news.beans.rule;

import java.util.List;

/**
 * 定时组合条件条件封装对象
 *
 * @author HDF
 * @version 1.0
 * @date 2022/1/11 14:16
 */
public class TimerCondition {
    /**
     * 初始化
     *
     * @param timeLate
     * @param eventCombinationConditionList
     * @param triggerTime
     */
    public TimerCondition(Long timeLate, List<EventCombinationCondition> eventCombinationConditionList, Long triggerTime) {
        this.timeLate = timeLate;
        this.eventCombinationConditionList = eventCombinationConditionList;
        this.triggerTime = triggerTime;
    }

    /**
     * 初始化
     *
     * @param timeLate
     * @param eventCombinationConditionList
     */
    public TimerCondition(Long timeLate, List<EventCombinationCondition> eventCombinationConditionList) {
        this.timeLate = timeLate;
        this.eventCombinationConditionList = eventCombinationConditionList;
    }

    /**
     * 需要延迟多久触发
     */
    private Long timeLate;
    /**
     * 行为组合规则条件
     */
    private List<EventCombinationCondition> eventCombinationConditionList;

    /**
     * 真正的触发时间
     */
    private Long triggerTime;

    public Long getTimeLate() {
        return timeLate;
    }

    public void setTimeLate(Long timeLate) {
        this.timeLate = timeLate;
    }

    public Long getTriggerTime() {
        return triggerTime;
    }

    public void setTriggerTime(Long triggerTime) {
        this.triggerTime = triggerTime;
    }

    public List<EventCombinationCondition> getEventCombinationConditionList() {
        return eventCombinationConditionList;
    }

    public void setEventCombinationConditionList(List<EventCombinationCondition> eventCombinationConditionList) {
        this.eventCombinationConditionList = eventCombinationConditionList;
    }
}
