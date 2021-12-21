package com.bigdata.rulematch.java.datagen;

import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
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
    public static RuleCondition getRuleConditions() throws ParseException {

        String ruleId = "rule_001";
        String keyByFields = "userId";

        /**
         * 触发事件条件
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
         * 行为次数条件列表  2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
         */
        //配置条件属性
        String actionCountConditionStartTimeStr = "2021-12-10 00:00:00";
        String actionCountConditionEndTimeStr = "9999-12-31 00:00:00";
        Long actionCountConditionStartTime = DateUtils.parseDate(actionCountConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();
        Long actionCountConditionEndTime = DateUtils.parseDate(actionCountConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();

        Map<String, String> actionCountCondition1Map = new HashMap<String, String>();
        actionCountCondition1Map.put("productId", "A");
        //生成加入购物车次数条件
        EventCondition actionCountCondition1 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionCountCondition1Map, actionCountConditionStartTime, actionCountConditionEndTime, 3, Integer.MAX_VALUE);

        Map<String, String> actionCountCondition2Map = new HashMap<String, String>();
        actionCountCondition2Map.put("productId", "A");
        //生成收藏次数条件
        EventCondition actionCountCondition2 = new EventCondition(EventRuleConstant.EVENT_COLLECT,
                actionCountCondition2Map, actionCountConditionStartTime, actionCountConditionEndTime, 5, Integer.MAX_VALUE);

        //条件列表
        EventCondition[] actionCountConditionList = new EventCondition[]{actionCountCondition1, actionCountCondition2};

        /**
         * 行为次序类条件列表 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
         */
        //配置条件属性
        String actionSeqConditionStartTimeStr = "2021-12-18 00:00:00";
        String actionSeqConditionEndTimeStr = "9999-12-31 00:00:00";

        Long actionSeqConditionStartTime = DateUtils.parseDate(actionSeqConditionStartTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();
        Long actionSeqConditionEndTime = DateUtils.parseDate(actionSeqConditionEndTimeStr, "yyyy-MM-dd HH:mm:ss").getTime();

        Map<String, String> actionSeqCondition1Map = new HashMap<String, String>();
        actionSeqCondition1Map.put("pageId", "A");
        //生成浏览页面条件
        EventCondition actionSeqCondition1 = new EventCondition(EventRuleConstant.EVENT_PAGE_VIEW,
                actionSeqCondition1Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        Map<String, String> actionSeqCondition2Map = new HashMap<String, String>();
        actionSeqCondition2Map.put("productId", "B");
        //生成加入购物车条件
        EventCondition actionSeqCondition2 = new EventCondition(EventRuleConstant.EVENT_ADD_CART,
                actionSeqCondition2Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        Map<String, String> actionSeqCondition3Map = new HashMap<String, String>();
        actionSeqCondition3Map.put("productId", "B");
        //生成提交订单条件
        EventCondition actionSeqCondition3 = new EventCondition(EventRuleConstant.EVENT_ORDER_SUBMIT,
                actionSeqCondition3Map, actionSeqConditionStartTime, actionSeqConditionEndTime);

        EventCondition[] actionSeqConditionList = new EventCondition[]{actionSeqCondition1, actionSeqCondition2, actionSeqCondition3};

        /**
         * 封装条件作为规则并返回
         */
        return new RuleCondition(ruleId, keyByFields, triggerCondition, userProfileConditions,
                actionCountConditionList, actionSeqConditionList);
    }
}
