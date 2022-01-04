package com.bigdata.rulematch.java.news.functions;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.RuleMatchResult;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.utils.StateDescUtils;
import com.bigdata.rulematch.java.news.controller.TriggerModelRuleMatchController;
import com.bigdata.rulematch.java.news.datagen.RuleConditionEmulator;
import com.bigdata.rulematch.java.news.conf.EventRuleConstant;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  17:59
 */
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());
    //
    private PropertiesConfiguration config = EventRuleConstant.config;
    //flink stage 事件列表
    private ListState<EventLogBean> eventListState = null;
    //条件匹配控制器
    private TriggerModelRuleMatchController triggerModelRuleMatchController = null;
    //规则条件数组（有可能存在多个规则）
    private RuleCondition[] ruleConditionArray = null;

    @Override
    public void open(Configuration parameters) throws Exception {
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

                logger.info("所有规则匹配成功,准备输出匹配结果信息...");

                //创建规则匹配结果对象
                RuleMatchResult matchResult = new RuleMatchResult(context.getCurrentKey(), ruleCondition.getRuleId(), eventLogBean.getTimeStamp(), System.currentTimeMillis());

                //将匹配结果输出
                collector.collect(matchResult);
            }
        }

    }

    @Override
    public void close() throws Exception {
        //关闭连接
        triggerModelRuleMatchController.closeConnection();
    }
}
