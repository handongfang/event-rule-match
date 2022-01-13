package com.bigdata.rulematch.java.news.beans.rule;

import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import com.bigdata.rulematch.java.news.beans.rule.TimerCondition;
import org.apache.commons.math3.util.Pair;

import java.util.List;
import java.util.Map;

/**
 * 规则条件的封装对象,需要匹配的规则都封装在这个对象中
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  12:04
 */
public class RuleCondition {
    /**
     * 初始化
     * @param ruleId
     * @param keyByFields
     * @param triggerEventCondition
     * @param userProfileConditions
     * @param eventCombinationConditionList
     * @param matchLimit
     * @param timerConditionList
     */
    public RuleCondition(String ruleId, String keyByFields, EventCondition triggerEventCondition, Map<String, Pair<String, String>> userProfileConditions, EventCombinationCondition[] eventCombinationConditionList, int matchLimit, List<TimerCondition> timerConditionList) {
        this.ruleId = ruleId;
        this.keyByFields = keyByFields;
        this.triggerEventCondition = triggerEventCondition;
        this.userProfileConditions = userProfileConditions;
        this.eventCombinationConditionList = eventCombinationConditionList;
        this.matchLimit = matchLimit;
        this.timerConditionList = timerConditionList;
    }

    /**
     * 初始化
     *
     * @param ruleId
     * @param keyByFields
     * @param triggerEventCondition
     * @param userProfileConditions
     * @param eventCombinationConditionList
     */
    public RuleCondition(String ruleId, String keyByFields, EventCondition triggerEventCondition, Map<String, Pair<String, String>> userProfileConditions, EventCombinationCondition[] eventCombinationConditionList) {
        this.ruleId = ruleId;
        this.keyByFields = keyByFields;
        this.triggerEventCondition = triggerEventCondition;
        this.userProfileConditions = userProfileConditions;
        this.eventCombinationConditionList = eventCombinationConditionList;
    }

    /**
     * 规则Id
     */
    private String ruleId;
    /**
     * keyby的字段, 使用逗号分割，例如:  "province,city"
     */
    private String keyByFields;
    /**
     * 规则触发条件
     */
    private EventCondition triggerEventCondition;
    /**
     * 用户画像属性条件
     */
    private Map<String, Pair<String, String>> userProfileConditions;
    /**
     * 行为组合条件
     */
    private EventCombinationCondition[] eventCombinationConditionList;
    /**
     * 规则匹配推送次数限制
     */
    private int matchLimit = 0;
    /**
     * 定时组合条件
     */
    private List<TimerCondition> timerConditionList = null;


    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getKeyByFields() {
        return keyByFields;
    }

    public void setKeyByFields(String keyByFields) {
        this.keyByFields = keyByFields;
    }

    public EventCondition getTriggerEventCondition() {
        return triggerEventCondition;
    }

    public void setTriggerEventCondition(EventCondition triggerEventCondition) {
        this.triggerEventCondition = triggerEventCondition;
    }

    public Map<String, Pair<String, String>> getUserProfileConditions() {
        return userProfileConditions;
    }

    public void setUserProfileConditions(Map<String, Pair<String, String>> userProfileConditions) {
        this.userProfileConditions = userProfileConditions;
    }

    public EventCombinationCondition[] getEventCombinationConditionList() {
        return eventCombinationConditionList;
    }

    public void setEventCombinationConditionList(EventCombinationCondition[] eventCombinationConditionList) {
        this.eventCombinationConditionList = eventCombinationConditionList;
    }

    public int getMatchLimit() {
        return matchLimit;
    }

    public void setMatchLimit(int matchLimit) {
        this.matchLimit = matchLimit;
    }

    public List<TimerCondition> getTimerConditionList() {
        return timerConditionList;
    }

    public void setTimerConditionList(List<TimerCondition> timerConditionList) {
        this.timerConditionList = timerConditionList;
    }
}
