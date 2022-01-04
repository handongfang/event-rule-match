package com.bigdata.rulematch.java.news.beans;

/**
 * @author HanDongfang
 * @create 2021-12-19  22:31
 */
public class RuleMatchResult {

    /**
     * 初始化
     *
     * @param ruleId
     * @param ruleName
     * @param trigEventTimestamp
     * @param matchTimestamp
     */
    public RuleMatchResult(String ruleId, String ruleName, Long trigEventTimestamp, Long matchTimestamp) {
        this.ruleId = ruleId;
        this.ruleName = ruleName;
        this.trigEventTimestamp = trigEventTimestamp;
        this.matchTimestamp = matchTimestamp;
    }


    /**
     * 匹配上的规则Id
     */
    private String ruleId;

    /**
     * 匹配上的规则名称
     */
    private String ruleName;

    /**
     * 触发规则匹配的那个事件的时间
     */
    private Long trigEventTimestamp;

    /**
     * 规则真正被匹配上的时间
     */
    private Long matchTimestamp;


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

    public Long getTrigEventTimestamp() {
        return trigEventTimestamp;
    }

    public void setTrigEventTimestamp(Long trigEventTimestamp) {
        this.trigEventTimestamp = trigEventTimestamp;
    }

    public Long getMatchTimestamp() {
        return matchTimestamp;
    }

    public void setMatchTimestamp(Long matchTimestamp) {
        this.matchTimestamp = matchTimestamp;
    }

}
