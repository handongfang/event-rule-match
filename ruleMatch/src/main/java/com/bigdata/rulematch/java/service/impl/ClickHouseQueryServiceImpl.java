package com.bigdata.rulematch.java.service.impl;

import com.bigdata.rulematch.java.bean.rule.EventCondition;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void queryActionCountCondition(String keyByField, String keyByFieldValue, EventCondition eventCondition) {
        logger.debug("CK收到一个行为次数类查询条件,keyByField:{}, keyByFieldValue: {}, 规则条件:{}", keyByField, keyByFieldValue, eventCondition);

        String querySqlStr = eventCondition.getActionCountQuerySql();

        logger.debug("构造的clickhouse查询行为次数的sql语句: {}", querySqlStr);
    }
}
