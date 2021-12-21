package com.bigdata.rulematch.scala.bean.rule

/**
 *
 * 行为序列规则条件中，最原子的一个封装，封装“1个”事件条件
 * 要素：
 * 事件时间约束
 * 事件序列约束
 * 序列的查询sql
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:11
 */
case class EventSeqCondition(
                           /**
                            * 规则条件中的一个事件要求的发生时间段起始
                            */
                           var timeRangeStart: Long = 0L,

                           /**
                            * 规则条件中的一个事件要求的发生时间段终点
                            */
                           var timeRangeEnd: Long = 0L,

                           /**
                            * 这个序列中要求包含的事件条件
                            */
                           eventSeqList: List[EventCondition] = _,

                           /**
                            * 行为序列类规则的查询SQL语句, 可能会包含多个序列, 每个序列都需要查询一次
                            */
                           var actionSeqQuerySql: String = _
                         )
