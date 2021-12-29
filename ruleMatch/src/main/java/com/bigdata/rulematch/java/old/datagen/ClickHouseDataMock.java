package com.bigdata.rulematch.java.old.datagen;

import com.alibaba.fastjson.JSON;
import com.bigdata.rulematch.java.old.bean.EventLogBean;
import com.bigdata.rulematch.java.old.conf.EventRuleConstant;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;

/**
 * ClickHouse数据模拟与测试
 *
 * @author HanDongfang
 * @create 2021-12-19  22:54
 */
public class ClickHouseDataMock {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        //插入数据测试
        String jsonStr = "{\"userId\":\"u202112180001\",\"timeStamp\":1639839496220,\"eventId\":\"productView\",\"properties\":{\"pageId\":\"646\",\"productId\":\"157\",\"title\":\"爱得堡 男靴中高帮马丁靴秋冬雪地靴 H1878 复古黄 40码\",\"url\":\"https://item.jd.com/36506691363.html\"}}";

        EventLogBean eventLogBean = JSON.parseObject(jsonStr, EventLogBean.class);

        //eventLogDataToClickHouse(eventLogBean)

        sequenceMatchTest();
    }

    /**
     * 将 EventLogBean 数据插入 ClickHouse
     *
     * @param eventLogBean
     */
    public static void eventLogDataToClickHouse(EventLogBean eventLogBean) throws ClassNotFoundException, SQLException {

        Class.forName(EventRuleConstant.CLICKHOUSE_DRIVER_NAME);

        Connection conn = DriverManager.getConnection(EventRuleConstant.CLICKHOUSE_URL);

        String sqlStr = String.format("INSERT INTO %s (userId,eventId,timeStamp,properties) VALUES(?,?,?,?)", EventRuleConstant.CLICKHOUSE_TABLE_NAME);


        PreparedStatement pstmt = conn.prepareStatement(sqlStr);

        String jsonStr = JSON.toJSON(eventLogBean.getProperties()).toString();

        //只是为了测试, value中不要带特殊符号，尤其是 "
        jsonStr = StringUtils.replace(jsonStr, "\"", "'");

        pstmt.setString(1, eventLogBean.getUserId());
        pstmt.setString(2, eventLogBean.getEventId());
        pstmt.setLong(3, eventLogBean.getTimeStamp());
        pstmt.setString(4, jsonStr);

        pstmt.execute();

        DbUtils.closeQuietly(conn, pstmt, null);

    }

    /**
     * 路径匹配函数测试, 这个SQL是进行行为次序类规则匹配的关键
     */
    public static void sequenceMatchTest() throws ClassNotFoundException, SQLException {
        /**
         * 语法：
         * sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
         *
         * 参数：
         * pattern — 模式字符串
         * --->模式语法
         * (?N) — 在位置N匹配条件参数。 条件在编号 [1, 32] 范围。 例如, (?1) 匹配传递给 cond1 参数。
         * .* — 表示任意的非指定事件
         * (?t operator value) — 分开两个事件的时间。 例如： (?1)(?t>1800)(?2) 匹配彼此发生超过1800秒的事件。
         * 这些事件之间可以存在任意数量的任何事件。 您可以使用 >=, >, <, <=, == 运算符。
         * (?1)(?t<=15)(?2)即表示事件1和2发生的时间间隔在15秒以内。
         *
         * timestamp — 包含时间的列。典型的时间类型是： Date 和 DateTime。您还可以使用任何支持的 UInt 数据类型。
         * cond1, cond2 — 事件链的约束条件。 数据类型是： UInt8。 最多可以传递32个条件参数。
         * 该函数只考虑这些条件中描述的事件。 如果序列包含未在条件中描述的数据，则函数将跳过这些数据
         *
         * 下面的 .*(?1).*(?2).*(?3) 表示事件1，2，3前后可以有任意事件，也就是保证先后顺序即可，不需要紧密相邻
         *
         * 返回值:
         * 1，如果模式匹配。
         * 0，如果模式不匹配。
         *
         * 注意：
         * Aggregate function sequenceMatch requires at least 3 arguments
         * sequenceMatch至少需要3个参数，所以is_match1中多给了一个参数
         */
        StringBuilder ss = new StringBuilder();
        ss.append("SELECT\n" +
                "    userId,\n" +
                "    sequenceMatch('.*(?1).*(?2).*(?3)')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'adShow' ,\n" +
                "    eventId = 'addCart' ,\n" +
                "    eventId = 'collect' \n" +
                "   ) AS is_match3,\n" +
                "  sequenceMatch('.*(?1).*(?2)')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'adShow' ,\n" +
                "    eventId = 'addCart' \n" +
                "  ) AS is_match2,\n" +
                " sequenceMatch('.*(?1).*')(\n" +
                "    toDateTime(`timeStamp`),\n" +
                "    eventId = 'adShow' ,\n" +
                "    eventId = 'addCart'\n" +
                "  ) AS is_match1\n");

        ss.append("FROM " + EventRuleConstant.CLICKHOUSE_TABLE_NAME + "\n");

        ss.append("WHERE userId='u202112180001' AND  `timeStamp` > 1639756800000\n" +
                "  AND (\n" +
                "        (eventId='adShow' AND properties['adId']='10')\n" +
                "        OR\n" +
                "        (eventId = 'addCart' AND properties['pageId']='720')\n" +
                "        OR\n" +
                "        (eventId = 'collect' AND properties['pageId']='263')\n" +
                "    )\n" +
                "GROUP BY userId\n");

        String querySqlStr = ss.toString();

        System.out.println(querySqlStr);

        Class.forName(EventRuleConstant.CLICKHOUSE_DRIVER_NAME);

        Connection conn = DriverManager.getConnection(EventRuleConstant.CLICKHOUSE_URL);

        PreparedStatement pstmt = conn.prepareStatement(querySqlStr);

        ResultSet rs = pstmt.executeQuery();

        while (rs.next()) {
            String userId = rs.getString("userId");
            int isMatch3 = rs.getInt("is_match3");
            int isMatch2 = rs.getInt("is_match2");
            int isMatch1 = rs.getInt("is_match1");

            System.out.println(String.format("userId: %s, isMatch3: %d, isMatch2: %d, isMatch1: %d", userId, isMatch3, isMatch2, isMatch1));
        }

        DbUtils.closeQuietly(conn, pstmt, rs);

    }
}
