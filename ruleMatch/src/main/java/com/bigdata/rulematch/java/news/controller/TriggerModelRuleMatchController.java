package com.bigdata.rulematch.java.news.controller;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.beans.rule.TimerCondition;
import com.bigdata.rulematch.java.news.service.TriggerModeRuleMatchServiceImpl;
import com.bigdata.rulematch.java.news.utils.EventCompareUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 规则触发匹配控制类
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  17:30
 */
public class TriggerModelRuleMatchController {
    private Logger logger = LoggerFactory.getLogger(TriggerModelRuleMatchController.class.getName());

    private TriggerModeRuleMatchServiceImpl triggerModeRuleMatchService = null;

    public TriggerModelRuleMatchController(ListState<EventLogBean> eventListState) throws SQLException, IOException, ClassNotFoundException {
        triggerModeRuleMatchService = new TriggerModeRuleMatchServiceImpl(eventListState);
    }

    /**
     * 判断事件是否满足规则
     *
     * @param ruleCondition
     * @param event
     */
    public boolean ruleIsMatch(RuleCondition ruleCondition, EventLogBean event) throws Exception {
        //判断当前数据bean是否满足规则的触发事件条件
        EventCondition triggerEventCondition = ruleCondition.getTriggerEventCondition();
        boolean isMatch = EventCompareUtils.eventMatchCondition(event, triggerEventCondition);

        if (isMatch) {
            logger.debug("满足触发条件，继续判断用户画像");
            //先判断是否有用户画像条件
            Map<String, Pair<String, String>> userProfileConditions = ruleCondition.getUserProfileConditions();
            if (userProfileConditions != null && userProfileConditions.size() > 0) {
                //开始判断用户画像条件是否满足
                isMatch = triggerModeRuleMatchService.matchProfileCondition(event.getUserId(), userProfileConditions);
                if (!isMatch) {
                    logger.debug("画像条件不满足, 不再进行后续匹配, userId: {}, 画像条件: {}", event.getUserId(), userProfileConditions);
                }
            } else {
                logger.debug("没有设置画像条件,继续判断组合条件");
            }

            if (isMatch) {
                //获取组合条件，判断是否存在组合条件
                EventCombinationCondition[] eventCombinationConditionList = ruleCondition.getEventCombinationConditionList();
                if (eventCombinationConditionList != null && eventCombinationConditionList.length > 0) {
                    Iterator<EventCombinationCondition> eventCombinationConditionIterator = Arrays.stream(eventCombinationConditionList).iterator();

                    while (eventCombinationConditionIterator.hasNext() && isMatch) {
                        EventCombinationCondition eventCombinationCondition = eventCombinationConditionIterator.next();
                        //只要有一个组合条件不满足,循环就会终止
                        isMatch = triggerModeRuleMatchService.matchEventCombinationCondition(ruleCondition.getKeyByFields(), event, eventCombinationCondition);
                        // 暂时写死（多个组合条件之间的关系是“且”）,  后面会再优化, 多个组合条件之间, 有可能是 或、与、非 等关系
                        if (!isMatch) {
                            logger.debug("循环终止, userId: {}, 组合条件不满足: {}", event.getUserId(), eventCombinationCondition);
                        }
                    }
                } else {
                    logger.debug("没有设置组合条件,规则匹配完成");
                }
            }

        } else {
            logger.debug("不满足触发条件，不再继续判断 EventLogBean:{}", event);
        }

        return isMatch;
    }

    /**
     * 检查定时条件是否满足
     *
     * @param keyByValue
     * @param timerCondition
     * @param queryStartTime
     * @param queryEndTime
     * @return
     */
    public boolean isMatchTimeCondition(String keyByValue, TimerCondition timerCondition, Long queryStartTime, Long queryEndTime) throws Exception {
        boolean isMatch = true;

        List<EventCombinationCondition> eventCombinationConditionList = timerCondition.getEventCombinationConditionList();
        if (eventCombinationConditionList != null && eventCombinationConditionList.size() > 0) {
            /**
             * 当存在被定时检查的组合条件时需要进行查询匹配！！！
             * 例如当前规则的某些条件是定时条件（浏览A页面,10分钟内又添加B商品进购物车并下单B商品）
             * 就需要等待触发定时器,进而开始匹配查询这些定时后的组合条件是否现在满足了
             */
            Iterator<EventCombinationCondition> iterator = eventCombinationConditionList.iterator();
            while (iterator.hasNext() && isMatch) {
                EventCombinationCondition eventCombinationCondition = iterator.next();
                EventLogBean eventLogBean = new EventLogBean(keyByValue, "", queryEndTime, null);

                eventCombinationCondition.setTimeRangeStart(queryStartTime);
                eventCombinationCondition.setTimeRangeEnd(queryEndTime);

                isMatch = triggerModeRuleMatchService.matchEventCombinationCondition("userId", eventLogBean, eventCombinationCondition);

            }
        }

        return isMatch;
    }

    /**
     * 关闭程序中使用的各种连接对象
     */
    public void closeConnection() {
        try {
            triggerModeRuleMatchService.closeConnection();
        } catch (SQLException throwables) {
            logger.error("关闭连接错误: {}", throwables);
        }
    }
}
