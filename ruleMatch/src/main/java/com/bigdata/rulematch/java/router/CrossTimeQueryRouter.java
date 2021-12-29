package com.bigdata.rulematch.java.router;

import com.bigdata.rulematch.java.bean.EventLogBean;
import com.bigdata.rulematch.java.bean.rule.EventCondition;
import com.bigdata.rulematch.java.bean.rule.EventSeqCondition;
import com.bigdata.rulematch.java.bean.rule.RuleCondition;
import com.bigdata.rulematch.java.conf.EventRuleConstant;
import com.bigdata.rulematch.java.service.impl.ClickHouseQueryServiceImpl;
import com.bigdata.rulematch.java.service.impl.HBaseQueryServiceImpl;
import com.bigdata.rulematch.java.service.impl.StateQueryServiceImpl;
import com.bigdata.rulematch.java.utils.ConnectionUtils;
import com.bigdata.rulematch.java.utils.EventRuleCompareUtils;
import com.bigdata.rulematch.java.utils.CrossTimeQueryUtil;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListState;
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
 * @create 2021-12-28  17:08
 */
public class CrossTimeQueryRouter {
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
    /**
     * flinkState查询实现
     */
    private StateQueryServiceImpl stateQueryService = null;

    public CrossTimeQueryRouter() throws IOException, SQLException, ClassNotFoundException {
        // 初始化hbase连接
        hbaseConn = ConnectionUtils.getHBaseConnection();

        // 初始化clickhouse连接
        ckConn = ConnectionUtils.getClickHouseConnection();

        //初始化hbase查询服务对象
        hBaseQueryService = new HBaseQueryServiceImpl(hbaseConn);

        //初始化clickhouse查询服务对象
        clickHouseQueryService = new ClickHouseQueryServiceImpl(ckConn);

        //初始化flinkState查询服务对象
        stateQueryService = new StateQueryServiceImpl();
    }

    /**
     * 规则匹配的流程
     *
     * @param eventLogBean
     * @param keyByFiedValue
     * @param ruleCondition
     * @param eventListState
     * @return
     */
    public boolean ruleMatch(EventLogBean eventLogBean, String keyByFiedValue, RuleCondition ruleCondition, ListState<EventLogBean> eventListState) {
        boolean isMatch = false;

        //1, 判断是否满足规则触发条件
        if (EventRuleCompareUtils.eventMatchCondition(eventLogBean, ruleCondition.getTriggerEventCondition())) {
            logger.debug(String.format("满足规则的触发条件: %s", ruleCondition.getTriggerEventCondition()));
            //满足规则的触发条件,才继续进行其他规则条件的匹配

            isMatch = true;

            //2, 判断是否满足用户画像条件（hbase）
            Map<String, Pair<String, String>> userProfileConditions = ruleCondition.getUserProfileConditions();
            if (userProfileConditions != null && userProfileConditions.size() > 0) {
                //只有设置了用户画像类条件,才去查询
                logger.debug(String.format("开始匹配用户画像类规则条件: %s", userProfileConditions));

                //从hbase中查询，并判断是否匹配
                isMatch = hBaseQueryService.userProfileConditionIsMatch(eventLogBean.getUserId(), userProfileConditions);

            } else {
                logger.debug("没有设置用户画像规则类条件,继续向下匹配次数类条件");
                isMatch = true;
            }

            //3, 行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）
            if (isMatch) {
                //根据当前的查询时间（也可以根据事件中的时间）,获取查询分界点
                Long queryBoundPoint = CrossTimeQueryUtil.getBoundPoint(System.currentTimeMillis());

                EventCondition[] actionCountConditionList = ruleCondition.getActionCountConditionList();
                logger.debug("用户画像类条件满足或未设置用户画像条件,开始匹配行为次数类条件 ");
                if (actionCountConditionList != null && actionCountConditionList.length > 0) {
                    //只有设置了次数类条件,才去查询
                    logger.debug(String.format("开始匹配行为次数类条件: %s", userProfileConditions));

                    //开始根据查询分界点, 进行分段查询
                    for (EventCondition eventCondition : actionCountConditionList) {
                        //条件时间起始范围在查询分界点右侧,则只需要查询flink state
                        if (eventCondition.getTimeRangeStart() >= queryBoundPoint) {
                            Long matchCount = 0L;
                            try {
                                matchCount = stateQueryService.stateQueryEventCount(eventListState, eventCondition, eventCondition.getTimeRangeStart(), eventCondition.getTimeRangeEnd());
                            } catch (Exception e) {
                                logger.error("状态查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", e);
                            }

                            //比较是否满足次数范围
                            if (matchCount < eventCondition.getMinLimit() || matchCount > eventCondition.getMaxLimit()) {
                                isMatch = false;
                            }
                        } else if (eventCondition.getTimeRangeEnd() <= queryBoundPoint) {
                            //条件时间结束范围在查询分界点左侧,则只需要查询clickHouse
                            Long queryCnt = null;
                            try {
                                queryCnt = clickHouseQueryService.queryActionCountCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventCondition);
                            } catch (SQLException throwables) {
                                logger.error("clickHouse查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", throwables);
                            }

                            //拿查询出来的行为次数与规则中要求的规则次数进行比较
                            if (queryCnt < eventCondition.getMinLimit() || queryCnt > eventCondition.getMaxLimit()) {
                                isMatch = false;
                            }
                        } else {
                            //查询分界点在条件时间范围内,则开始跨界查询
                            //先查flink state (分界点往后的查询state)
                            Long matchCountInState = 0L;
                            try {
                                matchCountInState = stateQueryService.stateQueryEventCount(eventListState, eventCondition, queryBoundPoint, eventCondition.getTimeRangeEnd());
                            } catch (Exception e) {
                                logger.error("状态查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", e);
                            }

                            if (matchCountInState < eventCondition.getMinLimit() || matchCountInState > eventCondition.getMaxLimit()) {
                                //state中不满足,再查clickhouse  (分界点往前的查询state)
                                Long matchCountInCK = 0L;
                                try {
                                    matchCountInCK = clickHouseQueryService.queryActionCountCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventCondition, eventCondition.getTimeRangeStart(), queryBoundPoint);
                                } catch (SQLException throwables) {
                                    logger.error("clickHouse查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", throwables);
                                }

                                //比较最终结果
                                Long matchCount = matchCountInState + matchCountInCK;
                                if (matchCount < eventCondition.getMinLimit() || matchCount > eventCondition.getMaxLimit()) {
                                    isMatch = false;
                                }

                            }
                        }
                        /*try {
                            Long countMax = clickHouseQueryService.queryActionCountCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventCondition);
                            if (countMax < eventCondition.getMinLimit() || countMax > eventCondition.getMaxLimit()) {
                                isMatch = false;
                                break;
                            }
                        } catch (SQLException throwables) {
                            logger.debug("查询clickHouse出错: ", throwables);
                            isMatch = false;
                            break;
                        }*/
                    }

                } else {
                    logger.debug("没有设置行为次数类规则条件,继续向下匹配次序类条件");
                    isMatch = true;
                }

                //4, 行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）
                if (isMatch) {
                    EventSeqCondition[] actionSeqConditionList = ruleCondition.getActionSeqConditionList();
                    logger.debug("行为次数类条件满足或未设置次数类条件,开始匹配行为次序类条件 ");
                    if (actionSeqConditionList != null && actionSeqConditionList.length > 0) {
                        //只有设置了次序类条件,才去查询
                        Iterator<EventSeqCondition> iterator = Arrays.stream(actionSeqConditionList).iterator();

                        //开始根据查询分界点, 进行分段查询
                        while (iterator.hasNext() && isMatch) {
                            EventSeqCondition eventSeqCondition = iterator.next();
                            if (eventSeqCondition.getTimeRangeStart() >= queryBoundPoint) {
                                int maxStep = 0;
                                try {
                                    maxStep = stateQueryService.stateQueryEventSequence(eventListState, eventSeqCondition, eventSeqCondition.getTimeRangeStart(), eventSeqCondition.getTimeRangeEnd());
                                } catch (Exception e) {
                                    logger.error("状态查询事件次序类条件失败,容易造成判断错误,可以采取旁路输出: {}", e);
                                }

                                //比较是否满足最大步骤
                                if (maxStep != eventSeqCondition.getEventSeqList().length) {
                                    isMatch = false;
                                }
                            } else if (eventSeqCondition.getTimeRangeEnd() <= queryBoundPoint) {
                                //条件时间结束范围在查询分界点左侧,则只需要查询clickHouse
                                int maxStep = 0;
                                try {
                                    maxStep = clickHouseQueryService.queryActionSeqCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventSeqCondition);
                                } catch (SQLException throwables) {
                                    logger.error("clickHouse查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", throwables);
                                }

                                //比较是否满足最大步骤
                                if (maxStep != eventSeqCondition.getEventSeqList().length) {
                                    isMatch = false;
                                }
                            } else {
                                //查询分界点在条件时间范围内,则开始跨界查询
                                //先查flink state (分界点往后的查询state)
                                int maxStepInState = 0;
                                try {
                                    maxStepInState = stateQueryService.stateQueryEventSequence(eventListState, eventSeqCondition, queryBoundPoint, eventSeqCondition.getTimeRangeEnd());
                                } catch (Exception e) {
                                    logger.error("状态查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", e);
                                }

                                //比较是否满足最大步骤,不满足继续查找CK
                                if (maxStepInState != eventSeqCondition.getEventSeqList().length) {
                                    //state中不满足,再查clickhouse  (分界点往前的查询state)
                                    int maxStepInCK = 0;
                                    try {
                                        maxStepInCK = clickHouseQueryService.queryActionSeqCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventSeqCondition, eventSeqCondition.getTimeRangeStart(), queryBoundPoint);
                                    } catch (SQLException throwables) {
                                        logger.error("clickHouse查询事件次数类条件失败,容易造成判断错误,可以采取旁路输出: {}", throwables);
                                    }

                                    //比较最终结果
                                    int maxStep = maxStepInState + maxStepInCK;
                                    if (maxStep != eventSeqCondition.getEventSeqList().length) {
                                        isMatch = false;
                                    }

                                }
                            }
                           /* try {
                                int matchMax = clickHouseQueryService.queryActionSeqCondition(ruleCondition.getKeyByFields(), keyByFiedValue, eventSeqCondition);
                                if (matchMax != eventSeqCondition.getEventSeqList().length) {
                                    isMatch = false;
                                }
                            } catch (SQLException throwables) {
                                logger.debug("查询clickHouse出错: ", throwables);
                                isMatch = false;
                                break;
                            }*/
                        }
                    } else {
                        logger.debug("没有设置行为次序类规则条件,匹配已完成...");
                        isMatch = true;
                    }

                    if (!isMatch) {
                        //最终还是不满足
                        logger.debug(String.format("不满足次序类规则条件: %s", ruleCondition.getActionSeqConditionList()));
                    }

                } else {
                    logger.debug(String.format("不满足行为次数类条件: %s", ruleCondition.getActionCountConditionList()));
                }

            } else {
                logger.debug(String.format("不满足用户画像类条件: %s", ruleCondition.getUserProfileConditions()));
                isMatch = true;
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
