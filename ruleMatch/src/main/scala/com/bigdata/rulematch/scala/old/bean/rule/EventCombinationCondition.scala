package com.bigdata.rulematch.scala.old.bean.rule

/**
 *
 * 事件组合体条件封装  类似于： [C !W F G](>=2)  [A.*B.*C]
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:14
 */
case class EventCombinationCondition(
                                      /**
                                       * 组合条件的发生时间区间起始
                                       */
                                      var timeRangeStart: Long = 0L,

                                      /**
                                       * 组合条件的发生时间区间结束
                                       */
                                      var timeRangeEnd: Long = 0L,

                                      /**
                                       * 组合发生的最小次数
                                       */
                                      var minLimit: Int = 0,

                                      /**
                                       * 组合发生的最大次数
                                       */
                                      var maxLimit: Int = 0,

                                      /**
                                       * 组合条件中关心的事件的列表
                                       */
                                      eventConditionList: List[EventCondition],

                                      /**
                                       * 查询的类型，比如ck表示查询clickhouse
                                       */
                                      sqlType: String,

                                      /**
                                       * 查询的sql语句
                                       */
                                      querySql: String
                                    )
