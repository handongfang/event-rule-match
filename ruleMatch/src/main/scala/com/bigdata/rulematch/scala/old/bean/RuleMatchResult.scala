package com.bigdata.rulematch.scala.old.bean

/**
 *
 * 规则匹配结果
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 15:08
 */
case class RuleMatchResult(
                            /**
                             * keyby的那个字段的值,如果是按照 userId keyby,那这个字段的值就是 userId的值
                             */
                            keyByValue: String,

                            /**
                             * 匹配上的规则Id
                             */
                            ruleId: String,

                            /**
                             * 触发规则匹配的那个事件的时间
                             */
                            trigEventTimestamp: Long,

                            /**
                             * 规则真正被匹配上的时间
                             */
                            matchTimestamp: Long
                          )
