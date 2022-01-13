package com.bigdata.rulematch.java.news.utils;

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.rule.RuleCondition;
import com.bigdata.rulematch.java.news.beans.rule.RuleTimer;
import com.bigdata.rulematch.java.news.beans.rule.RuleTimerV2;
import com.bigdata.rulematch.scala.news.beans.rule.MatchRule;
import org.apache.commons.math3.util.Pair;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;
import java.util.Map;

/**
 * 事件状态描述器工具
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-28  14:13
 */
public class StateDescUtils {
    /**
     * 获取一个近2小时事件状态描述器
     *
     * @return
     */
    public static ListStateDescriptor getEventBeanStateDesc() {
        ListStateDescriptor eventBeanListState = new ListStateDescriptor<EventLogBean>("eventBeanListState", TypeInformation.of(EventLogBean.class));

        //设置状态2小时有效
        StateTtlConfig stateTtlConfig = StateTtlConfig
                .newBuilder(Time.hours(2))
                //默认值：每次写入时‘创建state和更新state’初始化访问时间, OnCreateAndWrite
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                //默认值：从不返回过期数据,NeverReturnExpired
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        eventBeanListState.enableTimeToLive(stateTtlConfig);

        return eventBeanListState;
    }

    /**
     * 规则定时触发器描述
     */
    public static ListStateDescriptor<RuleTimer> ruleTimerStateDesc = new ListStateDescriptor<RuleTimer>("rule_timer", TypeInformation.of(RuleTimer.class));
    /**
     * 规则定时触发器描述V2
     */
    public static ListStateDescriptor<RuleTimerV2> ruleTimerStateDescV2 = new ListStateDescriptor<RuleTimerV2>("rule_timer", TypeInformation.of(RuleTimerV2.class));
}
