package com.bigdata.rulematch.java.news.dao;

import com.bigdata.rulematch.java.news.beans.rule.EventCombinationCondition;
import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import com.bigdata.rulematch.java.news.utils.EventCompareUtils;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * ClickHouse 查询器
 * 基于查询事件序列,返回组合条件事件匹配次数
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2022-01-04  13:17
 */
public class ClickHouseQuerier {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private Connection ckConn = null;

    //初始化连接
    public ClickHouseQuerier(Connection ckConn) {
        this.ckConn = ckConn;
    }

    /**
     * 在clickhouse中，根据组合条件及查询的时间范围，得到返回结果的1213形式字符串序列
     * queryRangeStart 和 queryRangeEnd这两个参数主要是为分段查询设计的，所以没有直接使用条件中的起止时间
     *
     * @param keyByField
     * @param keyByFieldValue
     * @param eventCombinationCondition
     * @param queryRangeStart
     * @param queryRangeEnd
     * @return
     */
    public String getEventCombinationConditionStr(String keyByField, String keyByFieldValue, EventCombinationCondition eventCombinationCondition, Long queryRangeStart, Long queryRangeEnd) throws SQLException {
        logger.debug("CK收到一个组合查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCombinationCondition);

        String querySqlStr = eventCombinationCondition.getQuerySql();
        logger.debug("构造的clickhouse查询行为次数的sql语句: {}", querySqlStr);

        EventCondition[] eventConditionList = eventCombinationCondition.getEventConditionList();

        ArrayList<String> eventIdList = new ArrayList<String>();

        for (EventCondition eventCondition : eventConditionList) {
            eventIdList.add(eventCondition.getEventId());
        }

        /*Stream<String> stringStream = Arrays.stream(eventConditionList).map(a -> {
            return a.getEventId();
        });

        String[] eventIdList = (String[]) stringStream.toArray();*/

        PreparedStatement pstmt = ckConn.prepareStatement(querySqlStr);

        pstmt.setString(1, keyByFieldValue);
        pstmt.setLong(2, queryRangeStart);
        pstmt.setLong(3, queryRangeEnd);

        ResultSet result = pstmt.executeQuery();

        StringBuilder eventIndexSeqBuilder = new StringBuilder();

        while (result.next()) {
            // 因为只查询了事件Id一个字段，所以直接用列索引号去取就可以
            String eventId = result.getString(1);
            // 因为查询的eventId,是依照组合条件中事件提前构建好的,故直接用事件列表索引。
            // 根据eventId到组合条件的事件列表中找对应的索引号,来作为最终结果拼接
            // 如果是原始的事件名称,比如add_cart, order_pay,进行正则匹配的时候比较麻烦, 所以先转换成 1212 这种形式
            eventIndexSeqBuilder.append(eventIdList.indexOf(eventId) + 1);
        }

        return eventIndexSeqBuilder.toString();
    }

    /**
     * 利用正则表达式查询组合条件满足的次数
     *
     * @param keyByField                keyby的时候使用的字段名称
     * @param keyByFieldValue           keyby的时候使用的字段对应的值
     * @param eventCombinationCondition 行为组合条件
     * @param queryRangeStart           查询时间范围起始
     * @param queryRangeEnd             查询时间范围结束
     * @throws SQLException
     */
    public int queryEventCombinationConditionCount(String keyByField, String keyByFieldValue, EventCombinationCondition eventCombinationCondition, Long queryRangeStart, Long queryRangeEnd) throws SQLException {
        //先查询满足条件的事件序列
        String eventIndexSeqStr = getEventCombinationConditionStr(keyByField, keyByFieldValue, eventCombinationCondition, queryRangeStart, queryRangeEnd);

        //取出组合条件中的正则表达式
        String matchPatternStr = eventCombinationCondition.getMatchPattern();

        int matchCount = EventCompareUtils.sequenceStrMatchRegexCount(eventIndexSeqStr, matchPatternStr);

        return matchCount;
    }

    /**
     * 关闭 clickhouse 连接
     *
     * @return
     */
    public void closeConnection() throws SQLException {
        DbUtils.close(ckConn);
    }
}
