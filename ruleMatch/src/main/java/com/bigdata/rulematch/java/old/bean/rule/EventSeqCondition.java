package com.bigdata.rulematch.java.old.bean.rule;

/**
 * 行为序列规则条件中，最原子的一个封装，封装“1个”序列条件
 * 要素：
 * 事件时间约束
 * 事件序列约束
 * 序列的查询sql
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-23  12:03
 */
public class EventSeqCondition {

    public EventSeqCondition(Long timeRangeStart, Long timeRangeEnd, EventCondition[] eventSeqList, String actionSeqQuerySql) {
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.eventSeqList = eventSeqList;
        this.actionSeqQuerySql = actionSeqQuerySql;
    }

    /**
     * 规则条件中的一个事件要求的发生时间段起始
     */
    private Long timeRangeStart = 0L;
    /**
     * 规则条件中的一个事件要求的发生时间段终点
     */
    private Long timeRangeEnd = 0L;
    /**
     * 这个序列中要求包含的事件条件
     */
    private EventCondition[] eventSeqList;
    /**
     * 行为序列类规则的查询SQL语句, 可能会包含多个序列, 每个序列都需要查询一次
     */
    private String actionSeqQuerySql;

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

    public EventCondition[] getEventSeqList() {
        return eventSeqList;
    }

    public void setEventSeqList(EventCondition[] eventSeqList) {
        this.eventSeqList = eventSeqList;
    }

    public String getActionSeqQuerySql() {
        return actionSeqQuerySql;
    }

    public void setActionSeqQuerySql(String actionSeqQuerySql) {
        this.actionSeqQuerySql = actionSeqQuerySql;
    }
}
