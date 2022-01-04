package com.bigdata.rulematch.java.news.conf;


import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;


/**
 * @author HanDongfang
 * @create 2021-12-19  17:18
 */
public class EventRuleConstant {

    /*// 得到一个类加载器对象
    public static ClassLoader loader = EventRuleConstant.class.getClassLoader();
    // 调用类加载器的getResourceAsStream方法读取文件资源
    public static InputStream ins = loader.getResourceAsStream("application.properties");
    // 特殊的键值对集合类，专用于读取properties配置文件的
    public static Properties config = new Properties();

    static {
        try {
            config.load(ins);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public static final Configurations configs = new Configurations();

    /**
     * 加载application.properties配置文件
     */
    public static PropertiesConfiguration config;

    static {
        try {
            config = configs.properties("application.properties");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    /**
     * Kafka相关的配置参数名称
     */
    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

    /**
     * ClickHouse相关配置名称
     */
    public static final String CLICKHOUSE_TABLE_NAME = "default.event_detail";
    public static final String CLICKHOUSE_DRIVER_NAME = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://82.156.210.70:8123/default";

    /**
     * HBase相关的配置名称
     */
    //hbase中用户画像表
    public static final String HBASE_USER_PROFILE_TABLE_NAME = "user-profile";
    //连接hbase的ZK地址
    public static final String HBASE_ZOOKEEPER_QUORU = "hbase.zookeeper.quorum";

    /**
     * MySql相关的配置名称
     */
    public static final String MYSQL_TABLE_NAME = "xxxxx-xxx";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/sobot?useUnicode=true&characterEncoding=UTF-8&useJDBCCompliantTimezoneShift=true";
    public static final String MYSQL_USER = "xxxxx";
    public static final String MYSQL_PASSWORD = "xxxxx";

    /**
     ********** 用户事件Id类型
     */

    /**
     * 浏览页面事件
     */
    public static final String EVENT_PAGE_VIEW = "pageView";
    /**
     * 浏览商品事件
     */
    public static final String EVENT_PRODUCT_VIEW = "productView";
    /**
     * 添加购物车事件
     */
    public static final String EVENT_ADD_CART = "addCart";
    /**
     * 收藏事件
     */
    public static final String EVENT_COLLECT = "collect";
    /**
     * 商品分享事件
     */
    public static final String EVENT_SHARE = "share";
    /**
     * 提交订单事件
     */
    public static final String EVENT_ORDER_SUBMIT = "orderSubmit";
    /**
     * 订单支付事件
     */
    public static final String EVENT_ORDER_PAY = "orderPay";

    /**
     ********** 用户画像规则比较符号
     */

    /**
     * 大于
     */
    public static final String OPERATOR_GREATERTHAN = "gt";
    /**
     * 大于等于
     */
    public static final String OPERATOR_GREATER_EQUEAL = "ge";
    /**
     * 小于
     */
    public static final String OPERATOR_LESSTHAN = "lt";
    /**
     * 小于等于
     */
    public static final String OPERATOR_LESS_EQUEAL = "le";
    /**
     * 等于
     */
    public static final String OPERATOR_EQUEAL = "eq";
    /**
     * 不等于
     */
    public static final String OPERATOR_NOT_EQUEAL = "neq";
    /**
     * 包含
     */
    public static final String OPERATOR_CONTAIN = "contain";
    /**
     * 不包含
     */
    public static final String OPERATOR_NOT_CONTAIN = "notContain";

}
