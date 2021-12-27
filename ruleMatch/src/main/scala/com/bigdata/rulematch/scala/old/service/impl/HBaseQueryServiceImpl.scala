package com.bigdata.rulematch.scala.old.service.impl

import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

/**
 * 查询hbase的服务实现类
 */
class HBaseQueryServiceImpl {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private var hbaseConn: Connection = null

  def this(hbaseConn: Connection) = {
    this()
    this.hbaseConn = hbaseConn
  }

  /**
   * 根据给定的rowkey，查询hbase，判断查询出来的标签值与规则中的标签值是否一致
   * @param rowkey
   * @param userProfileConditions
   * @return
   */
  def userProfileConditionIsMatch(rowkey: String,
                                  userProfileConditions: Map[String, (String, String)]) = {
    var isMatch = true

    //获取到hbase的表
    val table = hbaseConn.getTable(TableName.valueOf(EventRuleConstant.HBASE_USER_PROFILE_TABLE_NAME))

    val get = new Get(Bytes.toBytes(rowkey))
    val family = "f"

    //需要查询的用户标签,只需要查询规则条件中的标签即可,不需要全部查询
    val tags = userProfileConditions.keySet
    tags.foreach(tag => {
      get.addColumn(Bytes.toBytes(family), Bytes.toBytes(tag))
    })

    // 执行get查询
    val result = table.get(get)

    //判断查询出来的标签值与用户画像规则条件中的值是否一致
    val tagsIterator = tags.iterator
    while (tagsIterator.hasNext && isMatch) {
      val tag = tagsIterator.next()
      val valueBytes: Array[Byte] = result.getValue(Bytes.toBytes(family), Bytes.toBytes(tag))

      val queryValue = Bytes.toString(valueBytes)

      val userProfileTagOptAndValue = userProfileConditions.getOrElse(tag, (EventRuleConstant.OPERATOR_EQUEAL, ""))

      val userProfileTagOpt = userProfileTagOptAndValue._1
      val userProfileTagValue = userProfileTagOptAndValue._2

      if (!compareUserTag(userProfileTagOpt, userProfileTagValue, queryValue)) {
        isMatch = false
        logger.debug(s"用户画像类规则不匹配, 标签名: ${tag}, 规则中要求的值: ${userProfileTagOptAndValue}, " +
          s"hbase中查询到的值: ${queryValue}")
      }
    }

    isMatch
  }

  def compareUserTag(opt: String, optValue: String, queryValue: String) = {
    var isMatch = false

    val optVal = StringUtils.trim(optValue)

    //是否为数字
    var optValIsNumber = false
    //对应的数字
    var optValNumber = 0L
    //查询出来的数字
    var queryValNumber = 0L

    try {
      optValNumber = optVal.toLong

      queryValNumber = StringUtils.trim(queryValue).toLong

      optValIsNumber = true
    } catch {
      case _ =>
    }

    opt match {
      case EventRuleConstant.OPERATOR_CONTAIN =>{
        isMatch = StringUtils.contains(queryValue, optVal)
      }
      case EventRuleConstant.OPERATOR_NOT_CONTAIN =>{
        isMatch = !StringUtils.contains(queryValue, optVal)
      }
      case EventRuleConstant.OPERATOR_EQUEAL => {
        isMatch = StringUtils.equals(StringUtils.trim(queryValue), optVal)
      }
      case EventRuleConstant.OPERATOR_NOT_EQUEAL => {
        isMatch = !StringUtils.equals(StringUtils.trim(queryValue), optVal)
      }
      case EventRuleConstant.OPERATOR_GREATERTHAN => {
        if(optValIsNumber){

          if(queryValue > optVal) {
            isMatch = true
          }

        }else{
          logger.error(s"规则中要求的数值 ${optVal} 或者 查询结果中的数值: ${queryValue} 不是数字类型, 不支持的比较类型: ${opt}")
        }
      }
      case EventRuleConstant.OPERATOR_GREATER_EQUEAL => {
        if(optValIsNumber){

          if(queryValue >= optVal) {
            isMatch = true
          }

        }else{
          logger.error(s"规则中要求的数值 ${optVal} 或者 查询结果中的数值: ${queryValue} 不是数字类型, 不支持的比较类型: ${opt}")
        }
      }
      case EventRuleConstant.OPERATOR_LESSTHAN => {
        if(optValIsNumber){

          if(queryValue < optVal) {
            isMatch = true
          }

        }else{
          logger.error(s"规则中要求的数值 ${optVal} 或者 查询结果中的数值: ${queryValue} 不是数字类型, 不支持的比较类型: ${opt}")
        }
      }
      case EventRuleConstant.OPERATOR_LESS_EQUEAL => {
        if(optValIsNumber){

          if(queryValue <= optVal) {
            isMatch = true
          }

        }else{
          logger.error(s"规则中要求的数值 ${optVal} 或者 查询结果中的数值: ${queryValue} 不是数字类型, 不支持的比较类型: ${opt}")
        }
      }
      case _ => {
        logger.error(s"不支持的比较类型: ${opt}")
      }
    }

    isMatch
  }
}
