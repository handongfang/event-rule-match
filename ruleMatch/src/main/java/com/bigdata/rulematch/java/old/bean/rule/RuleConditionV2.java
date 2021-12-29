package com.bigdata.rulematch.java.old.bean.rule;

import java.util.Map;

/**
 * 规则条件的封装对象,需要匹配的规则都封装在这个对象中
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  12:04
 */
public class RuleConditionV2 {
    /**
     * 初始化
     *
     * @param ruleId
     * @param keyByFields
     * @param triggerEventCondition
     * @param userProfileConditions
     * @param eventCombinationConditionList
     */
    public RuleConditionV2(String ruleId, String keyByFields, EventCondition triggerEventCondition, Map<String, String> userProfileConditions, EventCombinationCondition[] eventCombinationConditionList) {
        this.ruleId = ruleId;
        this.keyByFields = keyByFields;
        this.triggerEventCondition = triggerEventCondition;
        this.userProfileConditions = userProfileConditions;
        this.eventCombinationConditionList = eventCombinationConditionList;
    }

    /**
     * 规则Id
     */
    private  String ruleId;
    /**
     * keyby的字段, 使用逗号分割，例如:  "province,city"
     */
    private  String keyByFields;
    /**
     * 规则触发条件
     */
    private  EventCondition triggerEventCondition;
    /**
     * 用户画像属性条件
     */
    private Map<String, String> userProfileConditions;
    /**
     * 行为组合条件
     */
    private EventCombinationCondition[] eventCombinationConditionList;

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

    public Map<String, String> getUserProfileConditions() {
        return userProfileConditions;
    }

    public void setUserProfileConditions(Map<String, String> userProfileConditions) {
        this.userProfileConditions = userProfileConditions;
    }

    public EventCombinationCondition[] getEventCombinationConditionList() {
        return eventCombinationConditionList;
    }

    public void setEventCombinationConditionList(EventCombinationCondition[] eventCombinationConditionList) {
        this.eventCombinationConditionList = eventCombinationConditionList;
    }
}
