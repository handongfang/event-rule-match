package com.bigdata.rulematch.scala.bean.rule

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
                            * 规则条件中的一个事件要求的发生次数最小值
                            */
                           var minLimit: Int = 0,


                           /**
                            * 规则条件中的一个事件要求的发生次数最大值
                            */
                           var maxLimit: Int = 0
                         )
