package com.bigdata.rulematch.java.old.service;

import com.bigdata.rulematch.java.old.conf.EventRuleConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 查询hbase的服务实现类
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  14:17
 */
public class HBaseQueryServiceImpl {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private Connection hbaseConn = null;

    /**
     * 初始化连接
     *
     * @param hbaseConn
     */
    public HBaseQueryServiceImpl(Connection hbaseConn) {
        this.hbaseConn = hbaseConn;
    }

    /**
     * 根据给定的rowkey，查询hbase，判断查询出来的标签值与规则中的标签值是否一致
     *
     * @param rowkey
     * @param userProfileConditions
     * @return
     */
    public boolean userProfileConditionIsMatch(String rowkey, Map<String, Pair<String, String>> userProfileConditions) {
        boolean isMatch = true;

        //获取到hbase的表
        Table table = null;
        try {
            table = hbaseConn.getTable(TableName.valueOf(EventRuleConstant.HBASE_USER_PROFILE_TABLE_NAME));
            //生成get
            Get get = new Get(Bytes.toBytes(rowkey));
            //列族
            String family = "f";
            Set<String> keySet = userProfileConditions.keySet();
            //添加查询列
            for (String s : keySet) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(s));
            }

            // 执行get查询
            Result result = table.get(get);

            for (Map.Entry<String, Pair<String, String>> userProfileEntry : userProfileConditions.entrySet()) {
                String key = userProfileEntry.getKey();
                byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(key));
                String valueStr = value.toString();

                Pair<String, String> userProfileTagOptAndValue = userProfileEntry.getValue();
                String userProfileTagOpt = userProfileTagOptAndValue.getKey();
                String userProfileValue = userProfileTagOptAndValue.getValue();

                if (!compareUserTag(userProfileTagOpt, valueStr, userProfileValue)) {
                    isMatch = false;
                    logger.debug("用户画像类规则不匹配, 标签名: {}, 规则中要求的值: {},hbase中查询到的值: {}", userProfileTagOpt, valueStr, userProfileValue);
                    break; //存在不匹配立即结束比较
                }
            }

            return isMatch;
        } catch (IOException e) {

            logger.error(String.format("获取hbase表数据出错: %s", e));
            return false;
        }
    }

    /**
     * 依据比较操作符号进行比对
     *
     * @param tagOpt
     * @param queryValueStr
     * @param ruleValueStr
     * @return
     */
    private boolean compareUserTag(String tagOpt, String queryValueStr, String ruleValueStr) {
        boolean isMatch = false;
        //是非数值
        boolean isNumber = false;
        //去掉两端空白
        queryValueStr = queryValueStr.trim();
        ruleValueStr = ruleValueStr.trim();
        int queryValue = 0;
        int ruleValue = 0;

        try {
            queryValue = Integer.valueOf(queryValueStr);
            ruleValue = Integer.valueOf(ruleValueStr);
            isNumber = true;
        } catch (NumberFormatException e) {
            logger.debug("非数值类型: ", e);
        }

        switch (tagOpt.toUpperCase()) {
            case EventRuleConstant.OPERATOR_CONTAIN:
                isMatch = StringUtils.contains(queryValueStr, ruleValueStr);
                break;

            case EventRuleConstant.OPERATOR_NOT_CONTAIN:
                isMatch = !StringUtils.contains(queryValueStr, ruleValueStr);
                break;

            case EventRuleConstant.OPERATOR_EQUEAL:
                isMatch = StringUtils.equals(queryValueStr, ruleValueStr);
                break;

            case EventRuleConstant.OPERATOR_NOT_EQUEAL:
                isMatch = !StringUtils.equals(queryValueStr, ruleValueStr);
                break;

            case EventRuleConstant.OPERATOR_GREATERTHAN:
                if (isNumber) {
                    isMatch = (queryValue > ruleValue);
                } else {
                    logger.error("非数值类型不能比较大小,queryValueStr:{},ruleValueStr{}", queryValueStr, ruleValueStr);
                }
                break;

            case EventRuleConstant.OPERATOR_GREATER_EQUEAL:
                if (isNumber) {
                    isMatch = (queryValue >= ruleValue);
                } else {
                    logger.error("非数值类型不能比较大小,queryValueStr:{},ruleValueStr{}", queryValueStr, ruleValueStr);
                }
                break;

            case EventRuleConstant.OPERATOR_LESSTHAN:
                if (isNumber) {
                    isMatch = (queryValue < ruleValue);
                } else {
                    logger.error("非数值类型不能比较大小,queryValueStr:{},ruleValueStr{}", queryValueStr, ruleValueStr);
                }
                break;

            case EventRuleConstant.OPERATOR_LESS_EQUEAL:
                if (isNumber) {
                    isMatch = (queryValue <= ruleValue);
                } else {
                    logger.error("非数值类型不能比较大小,queryValueStr:{},ruleValueStr{}", queryValueStr, ruleValueStr);
                }
                break;

            default:
                isMatch = false;
                logger.error("传入比较类型不正确: {}", tagOpt);
        }

        return isMatch;
    }
}
