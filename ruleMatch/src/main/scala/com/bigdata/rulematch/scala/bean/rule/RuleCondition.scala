package com.bigdata.rulematch.scala.bean.rule

/**
 *
 * 规则条件的封装对象,需要匹配的规则都封装在这个对象中 V1
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-19 23:08
 */
case class RuleCondition(
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
                          userProfileConditions: Map[String,String],

                          /**
                           * 行为次数类规则条件
                           */
                          actionCountConditionList: List[EventCondition],

                          /**
                           * 行为次序类条件
                           */
                          actionSeqConditionList: List[EventSeqCondition]
                        )
