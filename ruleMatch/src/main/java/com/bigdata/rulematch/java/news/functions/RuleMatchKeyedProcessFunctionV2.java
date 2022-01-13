package com.bigdata.rulematch.java.news.functions;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.RuleMatchResult;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.beans.rule.RuleTimer;
import com.bigdata.rulematch.java.news.beans.rule.RuleTimerV2;
import com.bigdata.rulematch.java.news.beans.rule.TimerCondition;
import com.bigdata.rulematch.java.news.conf.EventRuleConstant;
import com.bigdata.rulematch.java.news.controller.TriggerModelRuleMatchController;
import com.bigdata.rulematch.java.news.datagen.RuleConditionEmulator;
import com.bigdata.rulematch.java.news.utils.StateDescUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListState;
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
public class RuleMatchKeyedProcessFunctionV2 extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    //
    private PropertiesConfiguration config = EventRuleConstant.config;
    //规则触发定时器
    private ListState<RuleTimerV2> timerInfoState = null;
    //flink stage 事件列表
    private ListState<EventLogBean> eventListState = null;
    //条件匹配控制器
    private TriggerModelRuleMatchController triggerModelRuleMatchController = null;
    //规则条件数组（有可能存在多个规则）
    private RuleCondition[] ruleConditionArray = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //规则定时触发器状态
        timerInfoState = super.getRuntimeContext().getListState(StateDescUtils.ruleTimerStateDescV2);
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

                    List<TimerCondition> TList = new ArrayList<TimerCondition>();

                    //注册N个组合条件定时器,合成一个规则定时器,删除规则定时器状态时先要判断内部组合条件定时器是否为空
                    //多个延迟时间组合条件
                    for (TimerCondition timerCondition : timerConditionList) {
                        //单个组合条件触发时间
                        Long triggerTime = eventLogBean.getTimeStamp() + timerCondition.getTimeLate();

                        TimerCondition T = new TimerCondition(timerCondition.getTimeLate(), timerCondition.getEventCombinationConditionList(), triggerTime);

                        context.timerService().registerEventTimeTimer(triggerTime);

                        TList.add(T);
                    }

                    // 在规则定时信息state中进行记录
                    // 不同的规则，比如rule1和rule2都注册了一个10点的定时器,定时器触发的时候,需要知道应该检查哪个规则
                    timerInfoState.add(new RuleTimerV2(ruleCondition, TList));

                } else {
                    logger.info("所有规则匹配完毕,准备输出匹配结果信息...");
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
        Iterator<RuleTimerV2> ruleTimerIterator = timerInfoState.get().iterator();
        //用于更新规则定时器状态
        List<RuleTimerV2> timerInfoStateBak = new ArrayList<RuleTimerV2>();
        //获取所有规则定时器状态
        while (ruleTimerIterator.hasNext()) {
            RuleTimerV2 ruleTimerV2 = ruleTimerIterator.next();
            Pair<RuleCondition, List<TimerCondition>> ruleConditionListPair = ruleTimerV2.getrRleTimerStage();
            RuleCondition ruleCondition = ruleConditionListPair.getKey();
            //组合条件定时列表
            List<TimerCondition> TList = ruleConditionListPair.getValue();

            boolean isMatch = true;
            //遍历该规则中所有组合条件定时器状态
            /*Iterator<TimerCondition> iterator = TList.iterator();
            while (iterator.hasNext() && isMatch) {
                TimerCondition timerCondition = iterator.next();
                Long triggerTime = timerCondition.getTimeLate();

                if (triggerTime == timestamp) {
                    isMatch = triggerModelRuleMatchController.isMatchTimeCondition(ctx.getCurrentKey(), timerCondition,
                            timestamp - timerCondition.getTimeLate(), timestamp);
                    //当前组合条件定时器处理完成要移除
                    TList.remove(timerCondition);
                } else if (triggerTime < timestamp) {
                    // 增加删除过期定时信息的逻辑 - 双保险（一般情况下不会出现触发时间小于当前的记录）
                    TList.remove(timerCondition);
                }
            }*/
            //以上方案TList移除太慢(每次都需要迭代判断相等移除)
            for (int i = 0; i < TList.size(); i++) {
                TimerCondition timerCondition = TList.get(i);
                Long triggerTime = timerCondition.getTimeLate();

                if (triggerTime == timestamp) {
                    isMatch = triggerModelRuleMatchController.isMatchTimeCondition(ctx.getCurrentKey(), timerCondition,
                            timestamp - timerCondition.getTimeLate(), timestamp);
                    //提前跳出匹配
                    if (!isMatch) break;
                    //当前组合条件定时器处理完成要移除
                    TList.remove(i);
                } else if (triggerTime < timestamp) {
                    // 增加删除过期定时信息的逻辑 - 双保险（一般情况下不会出现触发时间小于当前的记录）
                    TList.remove(i);
                }
            }

            //只有当该规则所有组合定时器都触发完成才算满足该规则
            if (isMatch && (TList == null || TList.size() == 0)) {
                //创建规则匹配结果对象
                RuleMatchResult matchResult = new RuleMatchResult(ctx.getCurrentKey(), ruleCondition.getRuleId(), timestamp, System.currentTimeMillis());

                //将匹配结果输出
                out.collect(matchResult);
            }

            //当前规则定时组合条件为空或有定时组合条件未满足,清空该规则定时状态
            if (!isMatch || (TList == null || TList.size() == 0)) {
                ruleTimerIterator.remove();
            } else {
                //保留该规则定时器
                RuleTimerV2 ruleTimerV2Bak = new RuleTimerV2(ruleCondition, TList);
                timerInfoStateBak.add(ruleTimerV2Bak);
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
