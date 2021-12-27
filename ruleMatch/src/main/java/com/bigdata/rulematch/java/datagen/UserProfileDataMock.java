package com.bigdata.rulematch.java.datagen;

import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.scala.old.utils.ConnectionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 模拟用户画像数据生成,并插入HBase
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  13:55
 */
public class UserProfileDataMock {
    public static void main(String[] args) {
        Connection connection = null;
        Table table = null;

        try {
            //获取HBase连接对象
            connection = ConnectionUtils.getHBaseConnection();

            table = connection.getTable(TableName.valueOf(EventRuleConstant.HBASE_USER_PROFILE_TABLE_NAME));

            String userId = "u202112180001";

            String rowkey = userId;

            Put put = new Put(Bytes.toBytes(rowkey));

            byte[] family = Bytes.toBytes("f");

            put.addColumn(family, Bytes.toBytes("sex"), Bytes.toBytes("male"));
            put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes("18"));

            //插入数据
            table.put(put);

            if (table != null) {
                // 关闭连接
                table.close();
                connection.close();
            } else if (connection != null) {
                connection.close();
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
