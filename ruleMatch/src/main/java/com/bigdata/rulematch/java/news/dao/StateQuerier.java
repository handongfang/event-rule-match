package com.bigdata.rulematch.java.news.dao;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import com.bigdata.rulematch.java.news.utils.EventCompareUtils;
import org.apache.flink.api.common.state.ListState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink State状态查询器
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  14:11
 */
public class StateQuerier {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private ListState<EventLogBean> eventListState = null;

    //初始化状态
    public StateQuerier(ListState<EventLogBean> eventListState) {
        this.eventListState = eventListState;
    }

    /**
     * 在state中，根据组合条件及查询的时间范围，得到返回结果的1213形式字符串序列
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventCombinationCondition
     * @param queryRangeStart
     * @param queryRangeEnd
     */
    public String getEventCombinationConditionStr(String keyByField, String keyByFieldValue, EventCombinationCondition eventCombinationCondition, Long queryRangeStart, Long queryRangeEnd) throws Exception {
        //状态条件列表
        Iterable<EventLogBean> eventLogBeans = eventListState.get();
        //组合条件列表
        EventCondition[] eventConditionList = eventCombinationCondition.getEventConditionList();

        StringBuilder stringBuilder = new StringBuilder();

        //开始遍历状态事件日志(用于返回日志匹配的条件下标->1223)
        for (EventLogBean eventLogBean : eventLogBeans) {
            //判断遍历到的时间时间是否落在规则中要求的时间范围内
            if (eventLogBean.getTimeStamp() > queryRangeStart && eventLogBean.getTimeStamp() < queryRangeEnd) {
                //从组合条件列表中查询该事件是否存在匹配
                for (int index = 0; index < eventConditionList.length; index++) {
                    //如果事件匹配,记录匹配下标
                    if (EventCompareUtils.eventMatchCondition(eventLogBean, eventConditionList[index])) {
                        stringBuilder.append(index + 1);
                        //一旦一个事件匹配上了规则中的一个事件,则跳出当前循环,
                        break;
                    }
                }
            }
        }

        return stringBuilder.toString();
    }

    /**
     * 利用正则表达式查询组合条件满足的次数
     * queryRangeStart 和 queryRangeEnd这两个参数主要是为分段查询设计的，所以没有直接使用条件中的起止时间
     *
     * @param keyByField                keyby的时候使用的字段名称
     * @param keyByFieldValue           keyby的时候使用的字段对应的值
     * @param eventCombinationCondition 行为组合条件
     * @param queryRangeStart           查询时间范围起始
     * @param queryRangeEnd             查询时间范围结束
     */
    public int queryEventCombinationConditionCount(String keyByField, String keyByFieldValue, EventCombinationCondition eventCombinationCondition, Long queryRangeStart, Long queryRangeEnd) throws Exception {
        //先查询满足条件的事件序列
        String eventIndexSeqStr = getEventCombinationConditionStr(keyByField, keyByFieldValue, eventCombinationCondition, queryRangeStart, queryRangeEnd);

        //取出组合条件中的正则表达式
        String matchPatternStr = eventCombinationCondition.getMatchPattern();

        int matchCount = EventCompareUtils.sequenceStrMatchRegexCount(eventIndexSeqStr, matchPatternStr);

        return matchCount;
    }
}
