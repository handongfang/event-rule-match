package com.bigdata.rulematch.java.news.beans.rule;

import com.bigdata.rulematch.java.news.beans.rule.EventCondition;

/**
 * 作为次数与次序类条件的合并组合事件条件类
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  10:35
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
     * @param matchPattern
     * @param sqlType
     * @param querySql
     * @param cacheId
     */
    public EventCombinationCondition(Long timeRangeStart, Long timeRangeEnd, int minLimit, int maxLimit, EventCondition[] eventConditionList, String matchPattern, String sqlType, String querySql, String cacheId) {
        this.timeRangeStart = timeRangeStart;
        this.timeRangeEnd = timeRangeEnd;
        this.minLimit = minLimit;
        this.maxLimit = maxLimit;
        this.eventConditionList = eventConditionList;
        this.matchPattern = matchPattern;
        this.sqlType = sqlType;
        this.querySql = querySql;
        this.cacheId = cacheId;
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
     * 组合条件中关心的事件的列表,是次数类条件或次序类条件存放处
     */
    private EventCondition[] eventConditionList;
    /**
     * 组合条件未来计算要用的正则匹配表达式,用来描述各种规则的模式字符串
     */
    private String matchPattern;
    /**
     * 查询的类型，比如ck表示查询clickhouse
     */
    private String sqlType;
    /**
     * 查询的sql语句
     */
    private String querySql;
    /**
     * 条件缓存Id
     */
    private String cacheId;

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

    public String getMatchPattern() {
        return matchPattern;
    }

    public void setMatchPattern(String matchPattern) {
        this.matchPattern = matchPattern;
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

    public String getCacheId() {
        return cacheId;
    }

    public void setCacheId(String cacheId) {
        this.cacheId = cacheId;
    }
}
