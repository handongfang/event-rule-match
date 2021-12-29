package com.bigdata.rulematch.java.old.function;

import com.bigdata.rulematch.java.old.bean.EventLogBean;
import com.bigdata.rulematch.java.old.bean.RuleMatchResult;
import com.bigdata.rulematch.java.old.conf.EventRuleConstant;
import com.bigdata.rulematch.java.old.service.HBaseQueryServiceImpl;
import com.bigdata.rulematch.java.old.utils.ConnectionUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;

/**
 * 事件匹配规则写死,扩展行弱
 * @author HanDongfang
 * @create 2021-12-19  22:30
 */
public class RuleMatchKeyedProcessFunctionV1 extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private PropertiesConfiguration config  = EventRuleConstant.config;

    /**
     * hbase连接
     */
    private Connection hbaseConn  = null;
    /**
     * hbase查询实现
     */
    private HBaseQueryServiceImpl hBaseQueryService = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化hbase连接
        hbaseConn = ConnectionUtils.getHBaseConnection();

        //初始化hbase查询服务对象
        hBaseQueryService = new HBaseQueryServiceImpl(hbaseConn);

    }

    @Override
    public void processElement(EventLogBean eventLogBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        //1, 判断是否满足规则触发条件： 浏览A页面
        if (StringUtils.equals(EventRuleConstant.EVENT_PAGE_VIEW, eventLogBean.getEventId()) &&
                StringUtils.equals("A", eventLogBean.getProperties().get("pageId"))) {

            boolean isMatch = false;
            logger.debug(String.format("满足规则触发条件: ", "浏览A页面"));

            //2,判断是否满足用户画像条件  性别：女; 年龄: >18岁  （hbase）
            Map<String, Pair<String, String >> userProfileConditions = new HashMap<String, Pair<String, String > >();
            userProfileConditions.put("sex", new Pair<String, String >(EventRuleConstant.OPERATOR_EQUEAL,"female"));
            userProfileConditions.put("age", new Pair<String, String >(EventRuleConstant.OPERATOR_GREATER_EQUEAL,"18"));
            userProfileConditions.put("age", new Pair<String, String >(EventRuleConstant.OPERATOR_LESS_EQUEAL,"30"));

            if (userProfileConditions != null && userProfileConditions.size() > 0) {
                //只有设置了用户画像类条件,才去匹配
                logger.debug(String.format("开始匹配用户画像类规则条件: %s", userProfileConditions));

                //从hbase中查询，并判断是否匹配
                isMatch = hBaseQueryService.userProfileConditionIsMatch(eventLogBean.getUserId()
                        , userProfileConditions);

            } else {
                logger.debug("没有设置用户画像规则类条件");
            }

            //3，TODO 行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）
            if(isMatch){
                logger.debug("用户画像类条件满足,开始匹配行为次数类条件 ");
                /**
                 * 直接写多个次数事件,不利于后续的扩展。为降低耦合性采取规则封装
                 * 封装的是行为次数条件列表,一个次数条件它是由一个基本事件构成的[envet]
                 */
            }

            //4，TODO 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）
            if(isMatch){
                logger.debug("行为次数类条件满足,开始匹配行为次序类条件 ");
                /**
                 * 规则封装的是行为序列条件列表,一个序列条件它由多个基本事件构成的[envetSeq][envet]
                 */
            }

            if (isMatch) {

                RuleMatchResult matchResult = new RuleMatchResult("rule-001", "规则1", eventLogBean.getTimeStamp(), System.currentTimeMillis());

                collector.collect(matchResult);
            }

        }

    }

    @Override
    public void close() throws Exception {
        //关闭hbase连接
        try {
            hbaseConn.close();
        }finally {

        }
    }
}
