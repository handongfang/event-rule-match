package com.bigdata.rulematch.java.function;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.RuleMatchResult;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.java.datagen.RuleConditionEmulator;
import com.bigdata.rulematch.java.router.RuleMatchRouter;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;


/**
 * 封装规则匹配功能路由,便于后续控制匹配流程
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-27  14:47
 */
public class RuleMatchKeyedProcessFunctionV3 extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private PropertiesConfiguration config = EventRuleConstant.config;
    /**
     * 规则查询路由
     */
    private RuleMatchRouter ruleMatchRouter = null;


    @Override
    public void open(Configuration parameters) throws SQLException, IOException, ClassNotFoundException {
        // 初始化规则查询路由
        ruleMatchRouter = new RuleMatchRouter();
    }


    @Override
    public void processElement(EventLogBean eventLogBean, Context context, Collector<RuleMatchResult> collector) {
        //1. 获取模拟生成的规则
        RuleCondition ruleCondition = null;
        try {
            ruleCondition = RuleConditionEmulator.getRuleConditions(eventLogBean.getTimeStamp());
            logger.debug(String.format("获取到的规则条件: %s", ruleCondition));
        } catch (ParseException e) {
            logger.error("规则生成出错:", e);
        }

        if (ruleCondition != null) {
            //keyBy的值要获取到
            String keyByFiedValue = context.getCurrentKey();

            boolean isMatch = ruleMatchRouter.ruleMatch(eventLogBean, keyByFiedValue, ruleCondition);

            if (isMatch) {
                logger.debug("所有规则匹配成功,准备输出匹配结果信息...");

                //创建规则匹配结果对象
                RuleMatchResult matchResult = new RuleMatchResult("rule-001", "规则1", eventLogBean.getTimeStamp(), System.currentTimeMillis());

                //将匹配结果输出
                collector.collect(matchResult);
            }
        }
    }

    @Override
    public void close() {
        //关闭query连接
        ruleMatchRouter.closeConnection();
    }

}
