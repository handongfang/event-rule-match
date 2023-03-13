package com.bigdata.rulematch.java.news.functions;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.RuleMatchResult;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.beans.rule.RuleTimer;
import com.bigdata.rulematch.java.news.beans.rule.TimerCondition;
import com.bigdata.rulematch.java.news.utils.StateDescUtils;
import com.bigdata.rulematch.java.news.controller.TriggerModelRuleMatchController;
import com.bigdata.rulematch.java.news.datagen.RuleConditionEmulator;
import com.bigdata.rulematch.java.news.conf.EventRuleConstant;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 规则匹配处理程序
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  17:59
 */
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    //
    private PropertiesConfiguration config = EventRuleConstant.config;
    //规则触发定时器
    private ListState<RuleTimer> timerInfoState = null;
    //flink stage 事件列表
    private ListState<EventLogBean> eventListState = null;
    //条件匹配控制器
    private TriggerModelRuleMatchController triggerModelRuleMatchController = null;
    //规则条件数组（有可能存在多个规则）
    private RuleCondition[] ruleConditionArray = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //规则定时触发器状态
        timerInfoState = super.getRuntimeContext().getListState(StateDescUtils.ruleTimerStateDesc);
        //初始化用于存放2小时内事件明细的状态
        eventListState = super.getRuntimeContext().getListState(StateDescUtils.getEventBeanStateDesc());
        //初始化规则匹配控制器
        triggerModelRuleMatchController = new TriggerModelRuleMatchController(eventListState);
        //规则中所有条件起始时间,终止时间至今（采用最大值）
        long ruleStartTime = DateUtils.parseDate("2021-12-10 00:00:00").getTime();
        //获取模拟规则
        ruleConditionArray = RuleConditionEmulator.getRuleConditionArray(ruleStartTime);
    }

    @Override
    public void processElement(EventLogBean eventLogBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        //将当前收到 event 放入 flink 的state中，state设置的有效期为2小时
        eventListState.add(eventLogBean);

        //遍历所有规则，一个一个去匹配
        for (RuleCondition ruleCondition : ruleConditionArray) {
            logger.debug("获取到的规则条件: {}", ruleCondition);
            //判断是否满足规条件
            boolean isMatch = triggerModelRuleMatchController.ruleIsMatch(ruleCondition, eventLogBean);

            if (isMatch) {
                List<TimerCondition> timerConditionList = ruleCondition.getTimerConditionList();
                /**
                 * 获取规则内是否存在要延迟查询的组合条件列表
                 * 如果存在,创建该规则的定时器,触发时即可继续判断延迟时间后的条件是否满足。
                 */
                if (timerConditionList != null && timerConditionList.size() > 0) {
                    logger.info("开始注册规则中组合条件: {}的定时器", timerConditionList);

                    //注册定时器
                    //目前限定一个规则中只有一个时间组合条件
                    TimerCondition timerCondition = timerConditionList.get(0);

                    Long triggerTime = eventLogBean.getTimeStamp() + timerCondition.getTimeLate();

                    context.timerService().registerEventTimeTimer(triggerTime);

                    // 在规则定时信息state中进行记录
                    // 不同的规则，比如rule1和rule2都注册了一个10点的定时器,定时器触发的时候,需要知道应该检查哪个规则
                    timerInfoState.add(new RuleTimer(ruleCondition, triggerTime));

                } else {
                    logger.info("所有定时条件匹配完毕,准备输出匹配结果信息...");
                    //创建规则匹配结果对象
                    RuleMatchResult matchResult = new RuleMatchResult(context.getCurrentKey(), ruleCondition.getRuleId(), eventLogBean.getTimeStamp(), System.currentTimeMillis());
                    //将匹配结果输出
                    collector.collect(matchResult);
                }
            }
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RuleMatchResult> out) throws Exception {
        Iterator<RuleTimer> ruleTimerIterator = timerInfoState.get().iterator();
        //用于更新规则定时器状态
        List<RuleTimer> timerInfoStateBak = new ArrayList<RuleTimer>();
        //获取所有规则定时器状态
        while (ruleTimerIterator.hasNext()) {
            RuleTimer ruleTimer = ruleTimerIterator.next();
            Pair<RuleCondition, Long> ruleConditionLongPair = ruleTimer.getrRleTimerStage();
            RuleCondition ruleCondition = ruleConditionLongPair.getKey();
            Long triggerTime = ruleConditionLongPair.getValue();

            if (triggerTime == timestamp) {
                //说明是本次需要判断的定时条件，否则什么都不做
                List<TimerCondition> timerConditionList = ruleCondition.getTimerConditionList();

                //因为目前只支持一个定时条件,所以直接取第0个
                TimerCondition timerCondition = timerConditionList.get(0);

                boolean isMatch = triggerModelRuleMatchController.isMatchTimeCondition(ctx.getCurrentKey(), timerCondition,
                        timestamp - timerCondition.getTimeLate(), timestamp);

                // 清除已经检查完毕的规则定时点state信息
                ruleTimerIterator.remove();

                if (isMatch) {
                    //创建规则匹配结果对象
                    RuleMatchResult matchResult = new RuleMatchResult(ctx.getCurrentKey(), ruleCondition.getRuleId(), timestamp, System.currentTimeMillis());

                    //将匹配结果输出
                    out.collect(matchResult);
                }
            } else if (triggerTime < timestamp) {
                // 增加删除过期定时信息的逻辑 - 双保险（一般情况下不会出现触发时间小于当前的记录）
                ruleTimerIterator.remove();
            } else {
                //保留该规则定时器
                timerInfoStateBak.add(ruleTimer);
            }
        }
        timerInfoState.update(timerInfoStateBak);
    }

    @Override
    public void close() throws Exception {
        //关闭连接
        triggerModelRuleMatchController.closeConnection();
    }
}
