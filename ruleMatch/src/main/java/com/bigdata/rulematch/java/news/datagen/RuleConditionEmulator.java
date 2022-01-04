package com.bigdata.rulematch.java.news.datagen;

import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.conf.EventRuleConstant;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.math3.util.Pair;

import java.text.ParseException;
import java.util.*;

/**
 * 规则条件模拟
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  13:00
 */
public class RuleConditionEmulator {
    /**
     * 获取多个规则
     *
     * @param envetTimeStamp
     * @return
     * @throws ParseException
     */
    public static RuleCondition[] getRuleConditionArray(Long envetTimeStamp) throws ParseException {

        RuleCondition ruleCondition = getRuleConditions(envetTimeStamp);

        RuleCondition[] ruleConditions = {ruleCondition};

        return ruleConditions;
    }

    /**
     * 获取一个规则
     *
     * @param envetTimeStamp
     * @return
     * @throws ParseException
     */
    public static RuleCondition getRuleConditions(Long envetTimeStamp) throws ParseException {

        String ruleId = "rule_001";
        String keyByFields = "userId";

        /**
         * 触发判断的事件条件
         */
        Map<String, String> eventProps = new HashMap<String, String>();
        eventProps.put("pageId", "A");

        EventCondition triggerCondition = new EventCondition(EventRuleConstant.EVENT_PAGE_VIEW, eventProps);

        /**
         * 用户画像条件
         */
        Map<String, Pair<String, String>> userProfileConditions = new HashMap<String, Pair<String, String>>();
        userProfileConditions.put("sex", new Pair<String, String>(EventRuleConstant.OPERATOR_EQUEAL, "female"));
        userProfileConditions.put("age", new Pair<String, String>(EventRuleConstant.OPERATOR_GREATER_EQUEAL, "18"));
        userProfileConditions.put("age", new Pair<String, String>(EventRuleConstant.OPERATOR_LESS_EQUEAL, "30"));

        /**
         * 多个事件次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数大于等于3次,A商品收藏次数大于等于5次
         * 次数主要设置到组合条件里面,迎合次序条件模式匹配事件次数问题
         */
        //配置条件属性
        String actionCountConditionStartTimeStr = "2021-12-10 00:00:00";
        if (envetTimeStamp > 0L) {
            //当前事件时间
            Date date = new Date(envetTimeStamp);
            //DateUtils.addDays(date,-15);//前15天开始
            actionCountConditionStartTimeStr = DateFormatUtils.format(DateUtils.addMonths(date, -1).getTime(), "yyyy-MM-dd HH:mm:SS");
        }
        String actionCountConditionEndTimeStr = "9999-12-31 00:00:00";
        Long actionCountConditionStartTime = DateUtils.parseDate(actionCountConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();
        Long actionCountConditionEndTime = DateUtils.parseDate(actionCountConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();

        Map<String, String> actionCountCondition1Map = new HashMap<String, String>();
        actionCountCondition1Map.put("productId", "A");
        //生成加入购物车次数事件条件
        EventCondition actionCountCondition1 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 3, Integer.MAX_VALUE);

        //单个事件次数条件列表
        EventCondition[] eventConditionList1 = new EventCondition[]{actionCountCondition1};

        /**
         * 构建购物车事件次数查询语句
         */
        StringBuilder ss = new StringBuilder();
        ss.append("SELECT eventId \n");
        ss.append(String.format("FROM %s \n", EventRuleConstant.CLICKHOUSE_TABLE_NAME));
        ss.append(String.format("WHERE %s = ? AND properties['productId'] = 'A' \n", keyByFields));
        ss.append(String.format("AND eventId = '%s' \n", EventRuleConstant.EVENT_ADD_CART));
        ss.append("AND `timeStamp` BETWEEN ? AND ? \n");
        ss.append("ORDER BY `timeStamp`");

        String actionCountQuerySql = ss.toString();
        //匹配模式
        String matchPattern1 = "(1)";

        //构建事件组合条件1
        EventCombinationCondition eventCombinationCondition1 = new EventCombinationCondition(actionCountConditionStartTime, actionCountConditionEndTime, 3, Integer.MAX_VALUE,
                eventConditionList1, matchPattern1, "ck", actionCountQuerySql, "1001");

        Map<String, String> actionCountCondition2Map = new HashMap<String, String>();
        actionCountCondition2Map.put("productId", "A");
        //生成收藏次数事件条件
        EventCondition actionCountCondition2 = new EventCondition(EventRuleConstant.EVENT_COLLECT,
                actionCountCondition2Map, actionCountConditionStartTime, actionCountConditionEndTime, 5, Integer.MAX_VALUE);

        //单个事件次数条件列表
        EventCondition[] eventConditionList2 = new EventCondition[]{actionCountCondition2};

        /**
         * 构建收藏事件次数查询语句
         */
        ss.delete(0, ss.length());
        ss.append("SELECT eventId \n");
        ss.append(String.format("FROM %s \n", EventRuleConstant.CLICKHOUSE_TABLE_NAME));
        ss.append(String.format("WHERE %s = ? AND properties['productId'] = 'A' \n", keyByFields));
        ss.append(String.format("AND eventId = '%s' \n", EventRuleConstant.EVENT_COLLECT));
        ss.append("AND `timeStamp` BETWEEN ? AND ? \n");
        ss.append("ORDER BY `timeStamp`");

        actionCountQuerySql = ss.toString();
        //匹配模式
        String matchPattern2 = "(1)";

        //构建事件组合条件2
        EventCombinationCondition eventCombinationCondition2 = new EventCombinationCondition(actionCountConditionStartTime, actionCountConditionEndTime, 5, Integer.MAX_VALUE,
                eventConditionList2, matchPattern2, "ck", actionCountQuerySql, "1002");


        /**
         * 行为次序类条件列表 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
         * 组合条件次数范围表示该次序条件出现次数
         */
        //配置条件属性
        String actionSeqConditionStartTimeStr = "2021-12-18 00:00:00";
        if (envetTimeStamp > 0L) {
            //当前事件时间
            Date date = new Date(envetTimeStamp);
            //DateUtils.addDays(date,-3);//前3天开始
            actionSeqConditionStartTimeStr = DateFormatUtils.format(DateUtils.addHours(date, -3).getTime(), "yyyy-MM-dd HH:mm:SS");

        }
        String actionSeqConditionEndTimeStr = "9999-12-31 00:00:00";

        Long actionSeqConditionStartTime = DateUtils.parseDate(actionSeqConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();
        Long actionSeqConditionEndTime = DateUtils.parseDate(actionSeqConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();

        Map<String, String> actionSeqCondition1Map = new HashMap<String, String>();
        actionSeqCondition1Map.put("pageId", "A");
        //生成浏览页面事件条件
        EventCondition actionSeqCondition1 = new EventCondition(EventRuleConstant.EVENT_PAGE_VIEW,
                actionSeqCondition1Map, actionSeqConditionStartTime, actionSeqConditionEndTime, 1, Integer.MAX_VALUE);

        Map<String, String> actionSeqCondition2Map = new HashMap<String, String>();
        actionSeqCondition2Map.put("productId", "B");
        //生成加入购物车事件条件
        EventCondition actionSeqCondition2 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionSeqCondition2Map, actionSeqConditionStartTime, actionSeqConditionEndTime, 1, Integer.MAX_VALUE);

        Map<String, String> actionSeqCondition3Map = new HashMap<String, String>();
        actionSeqCondition3Map.put("productId", "B");
        //生成提交订单事件条件
        EventCondition actionSeqCondition3 = new EventCondition(EventRuleConstant.EVENT_ORDER_SUBMIT,
                actionSeqCondition3Map, actionSeqConditionStartTime, actionSeqConditionEndTime, 1, Integer.MAX_VALUE);

        //多个事件次序条件列表3
        EventCondition[] eventConditionList3 = new EventCondition[]{actionSeqCondition1, actionSeqCondition2, actionSeqCondition3};

        /**
         * 构建事件次序查询语句
         */
        ss.delete(0, ss.length());
        ss.append("SELECT eventId \n");
        ss.append("FROM " + EventRuleConstant.CLICKHOUSE_TABLE_NAME + "\n");
        ss.append(String.format("WHERE %s = ? AND `timeStamp` BETWEEN ? AND ? \n", keyByFields));
        ss.append("  AND ( \n");
        ss.append(String.format("        (eventId = '%s' AND properties['pageId'] = 'A')\n", EventRuleConstant.EVENT_PAGE_VIEW));
        ss.append("        OR \n");
        ss.append(String.format("        (eventId = '%s' AND properties['productId'] = 'B')\n", EventRuleConstant.EVENT_ADD_CART));
        ss.append("        OR \n");
        ss.append(String.format("        (eventId = '%s' AND properties['productId'] = 'B')\n", EventRuleConstant.EVENT_ORDER_SUBMIT));
        ss.append("    ) \n");
        ss.append("ORDER BY `timeStamp`");

        String actionSeqQuerySql = ss.toString();

        //用于匹配的模式
        String matchPattern3 = "(1.*2.*3)";

        //构建事件组合条件3
        EventCombinationCondition eventCombinationCondition3 = new EventCombinationCondition(actionSeqConditionStartTime, actionSeqConditionEndTime, 1, Integer.MAX_VALUE,
                eventConditionList2, matchPattern3, "ck", actionSeqQuerySql, "1003");


        //构建事件组合条件列表
        EventCombinationCondition[] eventCombinationConditionList = new EventCombinationCondition[]{eventCombinationCondition1, eventCombinationCondition2, eventCombinationCondition3};
        /**
         * 封装条件作为规则并返回
         */
        return new RuleCondition(ruleId, keyByFields, triggerCondition, userProfileConditions, eventCombinationConditionList);
    }
}
