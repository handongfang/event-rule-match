package com.bigdata.rulematch.java.news.beans.rule;

import org.apache.commons.math3.util.Pair;

/**
 * 规则定时条件封装对象
 *
 * @author HDF
 * @version 1.0
 * @date 2022/1/11 15:38
 */
public class RuleTimer {

    public RuleTimer(RuleCondition R, Long L) {
        this.ruleTimerStage = new Pair<RuleCondition, Long>(R, L);
    }

    /**
     * 规则定时触发
     */
    private Pair<RuleCondition, Long> ruleTimerStage = null;

    public Pair<RuleCondition, Long> getrRleTimerStage() {
        return ruleTimerStage;
    }

    public void setRuleTimer(Pair<RuleCondition, Long> ruleTimerStage) {
        this.ruleTimerStage = ruleTimerStage;
    }
}
