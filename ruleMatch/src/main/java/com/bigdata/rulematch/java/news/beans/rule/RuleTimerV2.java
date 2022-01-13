package com.bigdata.rulematch.java.news.beans.rule;

import org.apache.commons.math3.util.Pair;

import java.util.List;

/**
 * 规则定时条件封装对象V2
 * 解决每个规则只能有一个定时任务
 *
 * @author HDF
 * @version 1.0
 * @date 2022/1/11 18:34
 */
public class RuleTimerV2 {
    public RuleTimerV2(RuleCondition R, List<TimerCondition> TList) {
        this.ruleTimerStage = new Pair<RuleCondition, List<TimerCondition>>(R, TList);
    }

    /**
     * 规则定时触发
     */
    private Pair<RuleCondition, List<TimerCondition>> ruleTimerStage = null;

    public Pair<RuleCondition, List<TimerCondition>> getrRleTimerStage() {
        return ruleTimerStage;
    }

    public void setRuleTimer(Pair<RuleCondition, List<TimerCondition>> ruleTimerStage) {
        this.ruleTimerStage = ruleTimerStage;
    }
}
