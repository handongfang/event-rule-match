package com.bigdata.rulematch.java.utils;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.bigdata.rulematch.java.conf.EventRuleConstant;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  10:46
 */

/**
 * 各种连接对象的工具类
 */
public class ConnectionUtils {
    private static PropertiesConfiguration  config  = EventRuleConstant.config;
    /**
     * 获取 HBase 的连接对象
     */
    public static Connection getHBaseConnection() throws IOException {
        // 初始化hbase连接配置
        Configuration hbaseConf = HBaseConfiguration.create();

        hbaseConf.set(EventRuleConstant.HBASE_ZOOKEEPER_QUORU, config.getString(EventRuleConstant.HBASE_ZOOKEEPER_QUORU));

        Connection connection = ConnectionFactory.createConnection(hbaseConf);

        return connection;
    }

    /**
     * 获取 ClickHouse 连接对象
     */
    public java.sql.Connection getClickHouseConnection() throws ClassNotFoundException, SQLException {
        // 加载驱动类
        Class.forName(EventRuleConstant.CLICKHOUSE_DRIVER_NAME);

        java.sql.Connection connection = DriverManager.getConnection(EventRuleConstant.CLICKHOUSE_URL);

        return connection;
    }

    /**
     * 获取 MySql 连接对象
     */
    public java.sql.Connection getMySqlConnection() throws ClassNotFoundException, SQLException {
        // 加载驱动类
        Class.forName(EventRuleConstant.MYSQL_DRIVER_NAME);

        java.sql.Connection connection = DriverManager.getConnection(EventRuleConstant.MYSQL_URL,EventRuleConstant.MYSQL_USER,EventRuleConstant.MYSQL_PASSWORD);

        return connection;
    }
}
