package com.bigdata.rulematch.scala.bean.rule

/**
 *
 * 规则条件的封装对象,需要匹配的规则都封装在这个对象中
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:08
 */
case class RuleConditionV2(
                          /**
                           * 规则Id
                           */
                          ruleId: String,

                          /**
                           * keyby的字段, 使用逗号分割，例如:  "province,city"
                           */
                          keyByFields: String,

                          /**
                           * 规则触发条件
                           */
                          triggerEventCondition: EventCondition,

                          /**
                           * 用户画像属性条件
                           */
                          userProfileConditions: Map[String,(String, String)],

                          /**
                           * 行为组合条件
                           */
                          eventCombinationConditionList: List[EventCombinationCondition]
                        )
