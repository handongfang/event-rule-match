package com.bigdata.rulematch.java.function;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.RuleMatchResult;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author HanDongfang
 * @create 2021-12-19  22:30
 */
public class RuleMatchKeyedProcessFunctionV1 extends KeyedProcessFunction<String, EventLogBean, RuleMatchResult> {
    @Override
    public void processElement(EventLogBean eventLogBean, Context context, Collector<RuleMatchResult> collector) throws Exception {
        //1, 判断是否满足规则触发条件： 浏览A页面
        //    //过滤出满足触发条件的事件
        if(StringUtils.equals(EventRuleConstant.EVENT_PAGE_VIEW, eventLogBean.getEventId()) &&
                StringUtils.equals("A", eventLogBean.getProperties().get("pageId"))){

            boolean isMatch = false;

            //满足规则触发条件
            //2,判断是否满足用户画像条件  性别：女; 年龄: >18岁  （hbase）

            //3，行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）

            //4，行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）

            if(isMatch){

                RuleMatchResult matchResult = new RuleMatchResult("rule-001", "规则1", eventLogBean.getTimeStamp(), System.currentTimeMillis());

                collector.collect(matchResult);
            }

        }

    }
}
