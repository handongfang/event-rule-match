package com.bigdata.rulematch.scala.function

import com.bigdata.rulematch.scala.bean.{EventLogBean, RuleMatchResult}
import com.bigdata.rulematch.scala.conf.EventRuleConstant
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 *
 * 静态规则匹配KeyedProcessFunction
 *
 * 触发条件: 浏览A页面
 * 用户画像条件： 性别：女; 年龄: >18岁
 * 行为次数类条件：2021-12-10 00:00:00 至今, A商品加入购物车次数超过3次,A商品收藏次数大于5次
 * 行为次序类条件: 2021-12-18 00:00:00 至今, 用户依次浏览A页面->把B商品(商品Id为B)加入购物车->B商品提交订单
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:07
 */
class RuleMatchKeyedProcessFunctionV1 extends KeyedProcessFunction[String, EventLogBean, RuleMatchResult]{

  override def processElement(event: EventLogBean,
                              ctx: KeyedProcessFunction[String, EventLogBean, RuleMatchResult]#Context,
                              out: Collector[RuleMatchResult]): Unit = {

    //1, 判断是否满足规则触发条件： 浏览A页面
    //    //过滤出满足触发条件的事件
    if(StringUtils.equals(EventRuleConstant.EVENT_PAGE_VIEW, event.eventId) &&
      StringUtils.equals("A", event.properties.get("pageId"))){

      var isMatch = false

      //满足规则触发条件
      //2,判断是否满足用户画像条件  性别：女; 年龄: >18岁  （hbase）

      //3，行为次数类条件：A商品加入购物车次数超过3次,A商品收藏次数大于5次  （clickhouse）

      //4，行为次序类条件: 用户依次浏览A页面->把B商品(商品Id为pd001)加入购物车->B商品提交订单   （clickhouse）

      if(isMatch){

        //创建规则匹配结果对象
        val matchResult = RuleMatchResult("rule-001", "规则1", event.timeStamp, System.currentTimeMillis())

        //将匹配结果输出
        out.collect(matchResult)
      }

    }

  }
}
