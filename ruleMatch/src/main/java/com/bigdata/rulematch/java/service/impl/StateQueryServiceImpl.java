package com.bigdata.rulematch.java.service.impl;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.utils.EventRuleCompareUtils;
import org.apache.flink.api.common.state.ListState;

/**
 * flink state查询服务接口实现
 * queryStartTime和queryEndTime主要是考虑到跨界查询才添加的，而不是直接使用条件中的时间
 * 因为状态存储时间范围可能介于条件时间范围中,需要额外区分查询时间范围
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-28  16:25
 */
public class StateQueryServiceImpl {
    /**
     * 用于判断flink保存的状态List中的事件是否满足给定的次数类规则
     *
     * @param eventListState
     * @param countCondition
     * @param queryStartTime
     * @param queryEndTime
     * @return
     */
    public int stateQueryEventCount(ListState<EventLogBean> eventListState, EventCondition countCondition, Long queryStartTime, Long queryEndTime) throws Exception {
        //用于记录满足规则的事件个数
        int matchCount = 0;
        Iterable<EventLogBean> eventLogBeanIterable = eventListState.get();
        for (EventLogBean eventLogBean : eventLogBeanIterable) {
            //判断遍历到的时间时间是否落在规则中要求的时间范围内
            if (eventLogBean.getTimeStamp() > queryStartTime && eventLogBean.getTimeStamp() < queryEndTime) {
                //如果事件与规则匹配,匹配数则 +1
                if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, countCondition)) {
                    matchCount += 1;
                }
            }
        }

        return matchCount;
    }

    /**
     * 从flink状态List中查询给定的行为次序类规则最大满足到了第几步
     *
     * @param eventListState
     * @param eventSeqList
     * @param queryStartTime
     * @param queryEndTime
     * @return
     */
    public int stateQueryEventSequence(ListState<EventLogBean> eventListState, EventCondition[] eventSeqList, Long queryStartTime, Long queryEndTime) throws Exception {
        int maxStep = 0;

        Iterable<EventLogBean> eventLogBeanIterable = eventListState.get();

        for (EventLogBean eventLogBean : eventLogBeanIterable) {
            //判断遍历到的时间时间是否落在规则中要求的时间范围内
            if (eventLogBean.getTimeStamp() > queryStartTime && eventLogBean.getTimeStamp() < queryEndTime) {
                //如果事件与规则匹配,最大匹配步数则 +1
                if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, eventSeqList[maxStep])) {
                    maxStep += 1;
                }
            }
        }

        return maxStep;
    }
}
