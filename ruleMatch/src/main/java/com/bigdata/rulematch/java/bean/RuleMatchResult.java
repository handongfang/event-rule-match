package com.bigdata.rulematch.java.bean;

/**
 * @author 小韩韩
 * @create 2021-12-19  22:31
 */
public class RuleMatchResult {

    /**
     * 匹配上的规则Id
     */
    String ruleId;

    /**
     * 匹配上的规则名称
     */
    String ruleName;

    public RuleMatchResult(String ruleId, String ruleName) {
        this.ruleId = ruleId;
        this.ruleName = ruleName;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }


}
