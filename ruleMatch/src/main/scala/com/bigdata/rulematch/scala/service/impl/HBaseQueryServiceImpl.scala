package com.bigdata.rulematch.scala.service.impl

import com.bigdata.rulematch.scala.conf.EventRuleConstant
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
                                  userProfileConditions: Map[String, String]) = {
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
    //这里做了简化,正常情况下,Map的value可以使用一个二元组,第一个元素是比较符,第二个元素是比较的值
    //这样就会很灵活，比如判断年龄是否大于18岁,目前这种简化的方式，只能判断等于18岁
    val tagsIterator = tags.iterator
    while (tagsIterator.hasNext && isMatch) {
      val tag = tagsIterator.next()
      val valueBytes: Array[Byte] = result.getValue(Bytes.toBytes(family), Bytes.toBytes(tag))

      val valueStr = Bytes.toString(valueBytes)

      val userProfileConditionsValue = userProfileConditions.getOrElse(tag, "")

      if (!StringUtils.equals(valueStr, userProfileConditionsValue)) {
        isMatch = false
        logger.debug(s"用户画像类规则不匹配, 标签名: ${tag}, 规则中要求的值: ${userProfileConditionsValue}, " +
          s"hbase中查询到的值: ${valueStr}")
      }
    }

    isMatch
  }
}
