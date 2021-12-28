package com.bigdata.rulematch.java.service.impl;

import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.bean.rule.EventSeqCondition;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-23  11:00
 */
public class ClickHouseQueryServiceImpl {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private Connection ckConn = null;

    //初始化连接
    public ClickHouseQueryServiceImpl(Connection ckConn) {
        this.ckConn = ckConn;
    }

    /**
     * 指定keyBy键查询表中的行为数据次数
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventCondition
     */
    public Long queryActionCountCondition(String keyByField, String keyByFieldValue, EventCondition eventCondition) throws SQLException {
        return queryActionCountCondition(keyByField, keyByFieldValue, eventCondition, eventCondition.getTimeRangeStart(), eventCondition.getTimeRangeEnd());
    }

    /**
     * 为配合跨界查询而添加的时间范围
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventCondition
     * @param queryTimeStart
     * @param queryTimeEnd
     * @return
     */
    public Long queryActionCountCondition(String keyByField, String keyByFieldValue, EventCondition eventCondition, Long queryTimeStart, Long queryTimeEnd) throws SQLException {
        logger.debug("CK收到一个行为次数类查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCondition);

        String querySqlStr = eventCondition.getActionCountQuerySql();

        logger.debug("构造的clickhouse查询行为次数的sql语句: {}", querySqlStr);

        PreparedStatement pstmt = ckConn.prepareStatement(querySqlStr);

        pstmt.setString(1, keyByFieldValue);
        pstmt.setLong(2, queryTimeStart);
        pstmt.setLong(3, queryTimeEnd);

        ResultSet result = pstmt.executeQuery();

        Long count = 0L;
        while (result.next()) {
            count = result.getLong("cnt");
            System.out.println(String.format("查询到的行为次数: %d", count));
        }

        //不能关闭连接Connection,因为需要一直查,连接关闭在RuleMatchKeyedProcessFunction的close方法中
        DbUtils.closeQuietly(null, pstmt, result);

        return count;
    }

    /**
     * 根据给定的序列条件, 返回最大匹配的步骤号
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventSeqCondition
     * @return
     * @throws SQLException
     */
    public int queryActionSeqCondition(String keyByField, String keyByFieldValue, EventSeqCondition eventSeqCondition) throws SQLException {
        return queryActionSeqCondition(keyByField, keyByFieldValue, eventSeqCondition, eventSeqCondition.getTimeRangeStart(), eventSeqCondition.getTimeRangeEnd());
    }

    /**
     * 为配合跨界查询而添加的时间范围
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventSeqCondition
     * @param queryStartTime
     * @param queryEndTime
     * @return
     * @throws SQLException
     */
    public int queryActionSeqCondition(String keyByField, String keyByFieldValue, EventSeqCondition eventSeqCondition, Long queryStartTime, Long queryEndTime) throws SQLException {
        logger.debug("CK收到一个行为次序类查询条件,keyByField:{}, keyByFieldValue: {}, 规则序列:{}", keyByField, keyByFieldValue, eventSeqCondition);

        String querySqlStr = eventSeqCondition.getActionSeqQuerySql();

        logger.debug("构造的clickhouse查询行为次序的sql语句: {}", querySqlStr);

        PreparedStatement pstmt = ckConn.prepareStatement(querySqlStr);

        pstmt.setString(1, keyByFieldValue);
        pstmt.setLong(2, queryStartTime);
        pstmt.setLong(3, queryEndTime);

        ResultSet result = pstmt.executeQuery();

        /**
         * ┬─is_match3─┬─is_match2─┬─is_match1─┐
         * │         1 │         1 │         1 │
         * ┴───────────┴───────────┴───────────┘
         */

        //记录最大完成步骤
        int maxStep = 0;
        //其实result中只有一条数据
        if (result.next()) {
            //如果序列中有3个条件,那么需要依次判断is_match3,is_match2,is_match1是否为1
            //其实也就是 rs.getInt(1),rs.getInt(2),rs.getInt(3)是否为1
            //如果 rs.getInt(1) 等于1, 说明至多匹配了3步(3-0); 如果 rs.getInt(2) 等于1, 说明至多匹配了2步(3-1)
            //如果 rs.getInt(3) 等于1, 说明至多匹配了1步(3-2); 如果没有事件次序匹配则全等于0

            int i = 0;// i 要小于序列中的事件数
            boolean loopFlag = true;
            while (i < eventSeqCondition.getEventSeqList().length && loopFlag) {
                // rs.getInt(1) 就是最大匹配的那个
                if (result.getInt(i + 1) == 1) {
                    //一旦匹配了, 就是最大匹配步骤, 就可以退出了
                    maxStep = eventSeqCondition.getEventSeqList().length - i;

                    //跳出循环
                    loopFlag = false;
                }

                i += 1;
            }
        }

        return maxStep;
    }
}
