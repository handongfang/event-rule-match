package com.bigdata.rulematch.scala.old.bean.rule

/**
 *
 * 规则条件中，最原子的一个封装，封装“1个”事件条件
 * 要素：
 * 事件id
 * 事件属性约束
 * 事件时间约束
 * 事件次数约束
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:11
 */
case class EventCondition(
                           /**
                            * 规则条件中的一个事件的id
                            */
                           eventId: String,

                           /**
                            * 规则条件中的一个事件的属性约束
                            */
                           var eventProps: Map[String, String] = Map.empty[String, String],

                           /**
                            * 规则条件中的一个事件要求的发生时间段起始
                            */
                           var timeRangeStart: Long = 0L,

                           /**
                            * 规则条件中的一个事件要求的发生时间段终点
                            */
                           var timeRangeEnd: Long = 0L,


                           /**
                            * 规则条件中的一个事件要求的发生次数最小值,默认是0
                            */
                           var minLimit: Int = 0,


                           /**
                            * 规则条件中的一个事件要求的发生次数最大值,默认是Int的最大值
                            */
                           var maxLimit: Int = Int.MaxValue,

                           /**
                            * 行为次数类规则的查询SQL语句,次数类条件,每个事件都需要查询一次
                            * 封装在条件中,会比在规则匹配时生成,耦合性低
                            */
                           var actionCountQuerySql: String = ""
                         )
