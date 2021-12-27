package com.bigdata.rulematch.java.router;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.RuleMatchResult;
import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.bean.rule.EventSeqCondition;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.java.service.impl.ClickHouseQueryServiceImpl;
import com.bigdata.rulematch.java.service.impl.HBaseQueryServiceImpl;
import com.bigdata.rulematch.java.utils.ConnectionUtils;
import com.bigdata.rulematch.java.utils.EventRuleCompareUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.dbutils.DbUtils;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-27  15:04
 */
public class RuleMatchRouter {
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * 配置文件
     */
    private PropertiesConfiguration config = EventRuleConstant.config;

    /**
     * hbase连接
     */
    private Connection hbaseConn = null;
    /**
     * hbase查询实现
     */
    private HBaseQueryServiceImpl hBaseQueryService = null;
    /**
     * clickhouse连接
     */
    private java.sql.Connection ckConn = null;
    /**
     * clickhouse查询实现
     */
    private ClickHouseQueryServiceImpl clickHouseQueryService = null;

    public RuleMatchRouter() throws IOException, SQLException, ClassNotFoundException {
        // 初始化hbase连接
        hbaseConn = ConnectionUtils.getHBaseConnection();

        // 初始化clickhouse连接
        ckConn = ConnectionUtils.getClickHouseConnection();

        //初始化hbase查询服务对象
        hBaseQueryService = new HBaseQueryServiceImpl(hbaseConn);

        //初始化clickhouse查询服务对象
        clickHouseQueryService = new ClickHouseQueryServiceImpl(ckConn);
    }

    /**
     * 规则匹配的流程
     *
     * @param eventLogBean
     * @param keyByFiedValue
     * @param ruleCondition
     * @return
     * @throws SQLException
     */
    public boolean ruleMatch(EventLogBean eventLogBean, String keyByFiedValue, RuleCondition ruleCondition) {
        boolean isMatch = false;

        //1, 判断是否满足规则触发条件
        if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, ruleCondition.getTriggerEventCondition())) {
            logger.debug(String.format("满足规则的触发条件: %s", ruleCondition.getTriggerEventCondition()));
            //满足规则的触发条件,才继续进行其他规则条件的匹配

            isMatch = true;

            //2, 判断是否满足用户画像条件（hbase）
            Map<String, String> userProfileConditions = ruleCondition.getUserProfileConditions();
            if (userProfileConditions != null && userProfileConditions.size() > 0) {
                //只有设置了用户画像类条件,才去查询
                logger.debug(String.format("开始匹配用户画像类规则条件: %s", userProfileConditions));

                //从hbase中查询，并判断是否匹配
                isMatch = hBaseQueryService.userProfileConditionIsMatch(eventLogBean.getUserId(), userProfileConditions);

                //3, 行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）
                EventCondition[] actionCountConditionList = ruleCondition.getActionCountConditionList();
                if (isMatch) {
                    logger.debug("用户画像类条件满足,开始匹配行为次数类条件 ");
                    if (actionCountConditionList != null && actionCountConditionList.length > 0) {
                        //只有设置了次数类条件,才去查询
                        logger.debug(String.format("开始匹配行为次数类条件: %s", userProfileConditions));

                        //从clickHouse中查询，并判断是否匹配
                        for (EventCondition eventCondition : actionCountConditionList) {
                            try {
                                Long countMax = clickHouseQueryService.queryActionCountCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventCondition);
                                if (countMax < eventCondition.getMinLimit() || countMax > eventCondition.getMaxLimit()) {
                                    isMatch = false;
                                    break;
                                }
                            } catch (SQLException throwables) {
                                logger.debug("查询clickHouse出错: ", throwables);
                                isMatch = false;
                                break;
                            }
                        }
                    } else {
                        logger.debug("没有设置行为次数类规则条件,继续向下匹配次序类条件");
                    }

                    //4, 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）
                    EventSeqCondition[] actionSeqConditionList = ruleCondition.getActionSeqConditionList();
                    if (isMatch) {
                        logger.debug("行为次数类条件满足,开始匹配行为次序类条件 ");
                        if (actionSeqConditionList != null && actionSeqConditionList.length > 0) {
                            //只有设置了次序类条件,才去查询
                            Iterator<EventSeqCondition> iterator = Arrays.stream(actionSeqConditionList).iterator();

                            //从clickHouse中查询，并判断是否匹配
                            while (iterator.hasNext() && isMatch) {
                                EventSeqCondition eventSeqCondition = iterator.next();
                                try {
                                    int matchMax = clickHouseQueryService.queryActionSeqCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventSeqCondition);
                                    if (matchMax != eventSeqCondition.getEventSeqList().length) {
                                        isMatch = false;
                                    }
                                } catch (SQLException throwables) {
                                    logger.debug("查询clickHouse出错: ", throwables);
                                    isMatch = false;
                                    break;
                                }
                            }
                        } else {
                            logger.debug("没有设置行为次序类规则条件,匹配已完成...");
                        }

                        if (!isMatch) {
                            //最终还是不满足
                            logger.debug(String.format("不满足次序类规则条件: %s", actionSeqConditionList));
                        }

                    } else {
                        logger.debug(String.format("不满足行为次数类条件: %s", actionCountConditionList.toString()));
                    }

                } else {
                    logger.debug(String.format("不满足用户画像类条件: %s", ruleCondition.getUserProfileConditions()));
                }

            } else {
                logger.debug("没有设置用户画像规则类条件,继续向下匹配次数类条件");
            }

        } else {
            logger.debug(String.format("不满足规则的触发条件: %s", ruleCondition.getTriggerEventCondition()));
        }

        //返回结果
        return isMatch;
    }

    /**
     * 关闭连接
     */
    public void closeConnection() {
        logger.debug("关闭所有查询链接...");

        //关闭hbase连接
        if (hbaseConn != null) {
            try {
                hbaseConn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //关闭clickhouse连接
        DbUtils.closeQuietly(ckConn);
    }
}
