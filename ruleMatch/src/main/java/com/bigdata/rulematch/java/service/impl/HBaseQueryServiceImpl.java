package com.bigdata.rulematch.java.service.impl;

import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.lang.StringUtils;
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
    public boolean userProfileConditionIsMatch(String rowkey, Map<String, String> userProfileConditions) {
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

            for (Map.Entry<String, String> userProfileEntry : userProfileConditions.entrySet()) {
                String key = userProfileEntry.getKey();
                byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(key));
                String valueStr = value.toString();
                String userProfileEntryValue = userProfileEntry.getValue();
                //如果与查询值不匹配则停止比较并返回false
                if (!StringUtils.equals(valueStr, userProfileEntryValue)) {
                    isMatch = false;
                    break;
                }
            }

            return isMatch;
        } catch (IOException e) {

            logger.error(String.format("获取hbase表数据出错: %s", e));
            return false;
        }
    }
}
