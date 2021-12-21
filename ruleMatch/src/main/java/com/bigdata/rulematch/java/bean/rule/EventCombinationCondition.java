package com.bigdata.rulematch.java.bean.rule;

/**
 * 事件组合体条件封装  类似于： [C !W F G](>=2)  [A.*B.*C]
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  11:50
 */
public class EventCombinationCondition {
    /**
     * 初始化
     *
     * @param timeRangeStart
     * @param timeRangeEnd
     * @param minLimit
     * @param maxLimit
     * @param eventConditionList
     * @param sqlType
     * @param querySql
     */
    public EventCombinationCondition(Long timeRangeStart, Long timeRangeEnd, int minLimit, int maxLimit, EventCondition[] eventConditionList, String sqlType, String querySql) {
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
        this.eventConditionList = eventConditionList;
        this.sqlType = sqlType;
        this.querySql = querySql;
    }

    /**
     * 初始化
     *
     * @param timeRangeStart
     * @param timeRangeEnd
     * @param minLimit
     * @param maxLimit
     * @param eventConditionList
     */
    public EventCombinationCondition(Long timeRangeStart, Long timeRangeEnd, int minLimit, int maxLimit, EventCondition[] eventConditionList) {
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
        this.eventConditionList = eventConditionList;
    }

    /**
     * 初始化
     *
     * @param timeRangeStart
     * @param timeRangeEnd
     * @param eventConditionList
     */
    public EventCombinationCondition(Long timeRangeStart, Long timeRangeEnd, EventCondition[] eventConditionList) {
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.eventConditionList = eventConditionList;
    }

    /**
     * 组合条件的发生时间区间起始
     */
    private Long timeRangeStart = 0L;
    /**
     * 组合条件的发生时间区间结束
     */
    private Long timeRangeEnd = 0L;
    /**
     * 组合发生的最小次数
     */
    private int minLimit = 0;
    /**
     * 组合发生的最大次数
     */
    private int maxLimit = 0;
    /**
     * 组合条件中关心的事件的列表
     */
    private EventCondition[] eventConditionList;
    /**
     * 查询的类型，比如ck表示查询clickhouse
     */
    private String sqlType;
    /**
     * 查询的sql语句
     */
    private String querySql;


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

    public EventCondition[] getEventConditionList() {
        return eventConditionList;
    }

    public void setEventConditionList(EventCondition[] eventConditionList) {
        this.eventConditionList = eventConditionList;
    }

    public String getSqlType() {
        return sqlType;
    }

    public void setSqlType(String sqlType) {
        this.sqlType = sqlType;
    }

    public String getQuerySql() {
        return querySql;
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }
}
