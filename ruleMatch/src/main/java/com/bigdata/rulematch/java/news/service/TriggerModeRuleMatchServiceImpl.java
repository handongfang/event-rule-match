package com.bigdata.rulematch.java.news.service;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.dao.ClickHouseQuerier;
import com.bigdata.rulematch.java.news.dao.HbaseQuerier;
import com.bigdata.rulematch.java.news.dao.StateQuerier;
import com.bigdata.rulematch.java.news.utils.ConnectionUtils;
import com.bigdata.rulematch.java.news.utils.CrossTimeQueryUtil;
import com.bigdata.rulematch.scala.news.utils.EventUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

/**
 * 用于组合事件规则匹配的服务类
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  15:39
 */
public class TriggerModeRuleMatchServiceImpl {
    private Logger logger = LoggerFactory.getLogger(TriggerModeRuleMatchServiceImpl.class.getName());
    //flink stage 查询器
    private StateQuerier stateQuerier = null;
    //hbase 查询器
    private HbaseQuerier hbaseQuerier = null;
    //clickhouse 查询器
    private ClickHouseQuerier clickHouseQuerier = null;

    //初始化
    public TriggerModeRuleMatchServiceImpl(ListState<EventLogBean> eventListState) throws IOException, SQLException, ClassNotFoundException {
        this.stateQuerier = new StateQuerier(eventListState);

        hbaseQuerier = new HbaseQuerier(ConnectionUtils.getHBaseConnection());

        clickHouseQuerier = new ClickHouseQuerier(ConnectionUtils.getClickHouseConnection());
    }

    /**
     * 比较用户画像条件
     *
     * @param userId
     * @param userProfileConditions
     * @return
     */
    public boolean matchProfileCondition(String userId, Map<String, Pair<String, String>> userProfileConditions) {
        return hbaseQuerier.queryProfileConditionIsMatch(userId, userProfileConditions);
    }

    /**
     * 判断是否满足一个行为组合条件
     *
     * @param event
     * @param combinationCondition
     */
    public boolean matchEventCombinationCondition(String keyByField, EventLogBean event, EventCombinationCondition combinationCondition) throws Exception {
        //获取查询的分界时间点,如果使用事件时间,如果数据发生延迟,比如10点收到了6点10分的数据,那么查询分界点是5点,5点后的数据都查state,肯定查不到
        Long boundPointTime = CrossTimeQueryUtil.getBoundPoint(System.currentTimeMillis());
        // 判断规则条件的时间区间是否跨分界点
        Long conditionStart = combinationCondition.getTimeRangeStart();
        Long conditionEnd = combinationCondition.getTimeRangeEnd();

        logger.debug("分界时间点: {}, 组合条件要求的起始时间点: {}, 组合条件要求的起始时间点: {}"
                , DateFormatUtils.format(boundPointTime, "yyyy-MM-dd HH:mm:ss"),
                DateFormatUtils.format(conditionStart, "yyyy-MM-dd HH:mm:ss"),
                DateFormatUtils.format(conditionEnd, "yyyy-MM-dd HH:mm:ss"));

        boolean isMatch = false;

        if (conditionStart >= boundPointTime) {
            logger.debug("事件Id : {}, 只查询state", event.getEventId());
            // 查state状态
            int matchCount = stateQuerier.queryEventCombinationConditionCount(keyByField, event.getEventId(), combinationCondition,
                    conditionStart, conditionEnd);

            if (matchCount >= combinationCondition.getMinLimit() && matchCount <= combinationCondition.getMaxLimit()) {
                isMatch = true;
            }

        } else if (conditionEnd < boundPointTime) {
            logger.debug("事件Id : {}, 查询clickHouse", event.getEventId());
            //查询clickhouse
            int matchCount = clickHouseQuerier.queryEventCombinationConditionCount(keyByField, event.getEventId(), combinationCondition,
                    conditionStart, conditionEnd);

            if (matchCount >= combinationCondition.getMinLimit() && matchCount <= combinationCondition.getMaxLimit()) {
                isMatch = true;
            }

        } else {
            logger.debug("事件Id : {}, 跨区间查询", event.getEventId());
            //跨区间查询
            //先查state状态，看是否能提前结束
            int stateMatchCount = stateQuerier.queryEventCombinationConditionCount(keyByField, event.getEventId(), combinationCondition,
                    boundPointTime, conditionEnd);

            if (stateMatchCount >= combinationCondition.getMinLimit() && stateMatchCount <= combinationCondition.getMaxLimit()) {
                isMatch = true;
                logger.debug("事件Id : {}, 跨区间查询, state中已经满足条件, 规则匹配提前结束", event.getEventId());
            }

            if (!isMatch) {
                //如果只查state没有满足，再继续查询
                logger.debug("事件Id : {}, 跨区间查询, 只查询state中不满足条件, 继续进行规则匹配", event.getEventId());

                //查询ck中满足条件的事件序列->1223
                String ckEventSeqStr = clickHouseQuerier.getEventCombinationConditionStr(keyByField, event.getEventId(), combinationCondition,
                        conditionStart, boundPointTime);

                //查询state中的事件序列->1223
                String stateEventSeqStr = stateQuerier.getEventCombinationConditionStr(keyByField, event.getEventId(), combinationCondition,
                        boundPointTime, conditionEnd);

                //拼接事件序列
                int totalMatchCount = EventUtil.sequenceStrMatchRegexCount(ckEventSeqStr + stateEventSeqStr, combinationCondition.getMatchPattern());

                if (totalMatchCount >= combinationCondition.getMinLimit() && totalMatchCount <= combinationCondition.getMaxLimit()) {
                    isMatch = true;
                }
            }

        }

        return isMatch;
    }

    /**
     * 关闭程序中使用的各种连接对象
     */
    public void closeConnection() throws SQLException {
        hbaseQuerier.closeConnection();
        clickHouseQuerier.closeConnection();
    }
}
