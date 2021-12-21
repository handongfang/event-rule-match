package com.bigdata.rulematch.java.function;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.RuleMatchResult;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.java.service.HBaseQueryServiceImpl;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.datagen.RuleConditionEmulator;
import com.bigdata.rulematch.java.utils.ConnectionUtils;
import com.bigdata.rulematch.java.utils.EventRuleCompareUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 静态规则匹配KeyedProcessFunction 版本2（对规则的封装处理）
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  16:00
 */
public class RuleMatchKeyedProcessFunctionV2 extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger  = LoggerFactory.getLogger(this.getClass().getName());

    private PropertiesConfiguration config  = EventRuleConstant.config;

    /**
     * hbase连接
     */
    private Connection hbaseConn = null;
    //hbase服务实现类
    private HBaseQueryServiceImpl hBaseQueryService  = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化hbase连接
        hbaseConn = ConnectionUtils.getHBaseConnection();

        //初始化hbase查询服务对象
        hBaseQueryService = new  HBaseQueryServiceImpl(hbaseConn);
    }


    @Override
    public void processElement(EventLogBean eventLogBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        //1. 获取模拟生成的规则
        RuleCondition ruleCondition = RuleConditionEmulator.getRuleConditions();

        logger.debug(String.format("获取到的规则条件: %s", ruleCondition));

        //2, 判断是否满足规则触发条件
        if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, ruleCondition.getTriggerEventCondition())) {
            logger.debug(String.format("满足规则的触发条件: %s", ruleCondition.getTriggerEventCondition()));
            //满足规则的触发条件,才继续进行其他规则条件的匹配

            boolean isMatch = false;

            //3, 判断是否满足用户画像条件（hbase）
            Map<String, String> userProfileConditions = ruleCondition.getUserProfileConditions();
            if (userProfileConditions != null && userProfileConditions.size() > 0) {
                //只有设置了用户画像类条件,才去匹配
                logger.debug(String.format("开始匹配用户画像类规则条件: %s", userProfileConditions));

                //从hbase中查询，并判断是否匹配
                isMatch = hBaseQueryService.userProfileConditionIsMatch(eventLogBean.getUserId(), userProfileConditions);

            } else {
                logger.debug("没有设置用户画像规则类条件");
            }

            //4, TODO 行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）

            //5, TODO 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）

            if (isMatch) {

                //创建规则匹配结果对象
                RuleMatchResult  matchResult = new RuleMatchResult("rule-001", "规则1", eventLogBean.getTimeStamp(), System.currentTimeMillis());

                //将匹配结果输出
                collector.collect(matchResult);
            }
        } else {
            logger.debug(String.format("不满足规则的触发条件: %s", ruleCondition.getTriggerEventCondition()));
        }

    }

    @Override
    public void close() throws Exception {
        //关闭hbase连接
        hbaseConn.close();
    }
}
