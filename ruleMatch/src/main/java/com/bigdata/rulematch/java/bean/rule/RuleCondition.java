package com.bigdata.rulematch.java.bean.rule;

import org.apache.commons.math3.util.Pair;

import java.util.Map;

/**
 * 规则条件的封装对象,需要匹配的规则都封装在这个对象中 V1
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  12:01
 */
public class RuleCondition {
    /**
     * 初始化
     *
     * @param ruleId
     * @param keyByFields
     * @param triggerEventCondition
     * @param userProfileConditions
     * @param actionCountConditionList
     * @param actionSeqConditionList
     */
    public RuleCondition(String ruleId, String keyByFields, EventCondition triggerEventCondition, Map<String, Pair<String, String>> userProfileConditions, EventCondition[] actionCountConditionList, EventSeqCondition[] actionSeqConditionList) {
        this.ruleId = ruleId;
        this.keyByFields = keyByFields;
        this.triggerEventCondition = triggerEventCondition;
        this.userProfileConditions = userProfileConditions;
        this.actionCountConditionList = actionCountConditionList;
        this.actionSeqConditionList = actionSeqConditionList;
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
     * 事件次数规则条件
     */
    private EventCondition[] actionCountConditionList;

    /**
     * 事件次序类条件
     */
    private EventSeqCondition[] actionSeqConditionList;


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

    public EventCondition[] getActionCountConditionList() {
        return actionCountConditionList;
    }

    public void setActionCountConditionList(EventCondition[] actionCountConditionList) {
        this.actionCountConditionList = actionCountConditionList;
    }

    public EventSeqCondition[] getActionSeqConditionList() {
        return actionSeqConditionList;
    }

    public void setActionSeqConditionList(EventSeqCondition[] actionSeqConditionList) {
        this.actionSeqConditionList = actionSeqConditionList;
    }

}
