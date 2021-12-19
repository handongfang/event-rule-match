package com.bigdata.rulematch.scala.bean

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
                             * 匹配上的规则Id
                             */
                            ruleId: String,

                            /**
                             * 匹配上的规则名称
                             */
                            ruleName: String,
                          )
