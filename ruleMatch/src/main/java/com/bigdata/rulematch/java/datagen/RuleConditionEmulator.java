package com.bigdata.rulematch.java.datagen;

import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.bean.rule.EventSeqCondition;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 规则条件模拟
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  13:00
 */
public class RuleConditionEmulator {
    /**
     * 获取一个规则
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
        Map<String, String> userProfileConditions = new HashMap<String, String>();
        userProfileConditions.put("sex", "female");
        userProfileConditions.put("ageStart", "18");
        userProfileConditions.put("ageEnd", "30");

        /**
         * 多个事件次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
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

        /**
         * 构建购物车次事件次数查询语句
         */
        StringBuilder ss = new StringBuilder();
        ss.append("SELECT count(1) AS cnt\n");
        ss.append(String.format("FROM %s \n", EventRuleConstant.CLICKHOUSE_TABLE_NAME));
        ss.append(String.format("WHERE %s = ? AND properties['productId'] = 'A' \n", keyByFields));
        ss.append(String.format("AND eventId = '%s' \n", EventRuleConstant.EVENT_ADD_CART));
        ss.append(String.format("AND `timeStamp` BETWEEN %s AND %s", actionCountConditionStartTime, actionCountConditionEndTime));

        String actionCountQuerySql = ss.toString();

        Map<String, String> actionCountCondition1Map = new HashMap<String, String>();
        actionCountCondition1Map.put("productId", "A");
        //生成加入购物车次数事件条件
        EventCondition actionCountCondition1 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 3, Integer.MAX_VALUE, actionCountQuerySql);

        /**
         * 构建收藏事件次数查询语句
         */
        ss.delete(0, ss.length());
        ss.append("SELECT count(1) AS cnt\n");
        ss.append(String.format("FROM %s \n", EventRuleConstant.CLICKHOUSE_TABLE_NAME));
        ss.append(String.format("WHERE %s = ? AND properties['productId'] = 'A' \n", keyByFields));
        ss.append(String.format("AND eventId = '%s' \n", EventRuleConstant.EVENT_COLLECT));
        ss.append(String.format("AND `timeStamp` BETWEEN %s AND %s", actionCountConditionStartTime, actionCountConditionEndTime));
        actionCountQuerySql = ss.toString();

        Map<String, String> actionCountCondition2Map = new HashMap<String, String>();
        actionCountCondition2Map.put("productId", "A");
        //生成收藏次数事件条件
        EventCondition actionCountCondition2 = new EventCondition(EventRuleConstant.EVENT_COLLECT,
                actionCountCondition2Map, actionCountConditionStartTime, actionCountConditionEndTime, 5, Integer.MAX_VALUE, actionCountQuerySql);

        //多个事件次数条件列表
        EventCondition[] actionCountConditionList = new EventCondition[]{actionCountCondition1, actionCountCondition2};


        /**
         * 行为次序类条件列表 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
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
                actionSeqCondition1Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        Map<String, String> actionSeqCondition2Map = new HashMap<String, String>();
        actionSeqCondition2Map.put("productId", "B");
        //生成加入购物车事件条件
        EventCondition actionSeqCondition2 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionSeqCondition2Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        Map<String, String> actionSeqCondition3Map = new HashMap<String, String>();
        actionSeqCondition3Map.put("productId", "B");
        //生成提交订单事件条件
        EventCondition actionSeqCondition3 = new EventCondition(EventRuleConstant.EVENT_ORDER_SUBMIT,
                actionSeqCondition3Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        //多个事件次序条件列表
        EventCondition[] eventSeqList = new EventCondition[]{actionSeqCondition1, actionSeqCondition2, actionSeqCondition3};

        /**
         * 构建事件次序查询语句
         */
        ss.delete(0, ss.length());
        ss.append("SELECT\n" +
                "    userId,\n" +
                "    sequenceMatch('.*(?1).*(?2).*(?3)')(\n" +
                "    toDateTime(`timeStamp`),\n");
        ss.append(String.format("    eventId = '%s' ,\n", EventRuleConstant.EVENT_PAGE_VIEW));
        ss.append(String.format("    eventId = '%s' ,\n", EventRuleConstant.EVENT_ADD_CART));
        ss.append(String.format("    eventId = '%s' \n", EventRuleConstant.EVENT_ORDER_SUBMIT));
        ss.append("  ) AS is_match3,\n");
        ss.append("  sequenceMatch('.*(?1).*(?2)')(\n" +
                "    toDateTime(`timeStamp`),\n");
        ss.append(String.format("    eventId = '%s' ,\n", EventRuleConstant.EVENT_PAGE_VIEW));
        ss.append(String.format("    eventId = '%s' \n", EventRuleConstant.EVENT_ADD_CART));
        ss.append("  ) AS is_match2,\n" +
                " sequenceMatch('.*(?1).*')(\n" +
                "    toDateTime(`timeStamp`),\n");
        ss.append(String.format("    eventId = '%s' ,\n", EventRuleConstant.EVENT_PAGE_VIEW));
        ss.append(String.format("    eventId = '%s' \n", EventRuleConstant.EVENT_ADD_CART));
        ss.append("    ) AS is_match1\n");
        ss.append("FROM " + EventRuleConstant.CLICKHOUSE_TABLE_NAME + "\n");
        ss.append(String.format("WHERE ${keyByFields} = ? AND `timeStamp` BETWEEN %s AND %s", actionSeqConditionStartTime, actionSeqConditionEndTime));
        ss.append("  AND (\n");
        ss.append(String.format("        (eventId='%s' AND properties['adId']='10')\n", EventRuleConstant.EVENT_PAGE_VIEW));
        ss.append("        OR\n");
        ss.append(String.format("        (eventId = '%s' AND properties['pageId']='720')\n", EventRuleConstant.EVENT_ADD_CART));

        ss.append("        OR\n");
        ss.append(String.format("        (eventId = '%s' AND properties['pageId']='263')\n", EventRuleConstant.EVENT_ORDER_SUBMIT));
        ss.append("    )\n");

        String actionSeqQuerySql = ss.toString();

        //生成多事件次序条件
        EventSeqCondition eventSeqCondition1 = new EventSeqCondition(actionSeqConditionStartTime, actionSeqConditionEndTime, eventSeqList, actionSeqQuerySql);

        //生成多序列条件
        EventSeqCondition[] actionSeqConditionList = new EventSeqCondition[]{eventSeqCondition1};

        /**
         * 封装条件作为规则并返回
         */
        return new RuleCondition(ruleId, keyByFields, triggerCondition, userProfileConditions,
                actionCountConditionList, actionSeqConditionList);
    }
}
