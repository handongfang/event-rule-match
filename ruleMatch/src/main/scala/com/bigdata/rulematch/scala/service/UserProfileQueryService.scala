package com.bigdata.rulematch.scala.service

import com.bigdata.rulematch.scala.conf.EventRuleConstant
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.{Cell, CellScanner, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}


/**
 *
 * 获取用户画像信息
 * @author Qiuzx
 * @date 2021/12/20 15:32
 * @version 1.0
 */
object UserProfileQueryService {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val hbaseZookeeperQuorum = "192.168.1.158:2181"
  private val tableName = "user-profile"
  private val family = "f"

//  def main(args: Array[String]): Unit = {
//    getUserProfileByRowkey("u")
//  }

  def getUserProfileByRowkey(rowKey: String ,userProfileConditions: Map[String,(String, String)]):Boolean = {
    var isMatch = false;
    val config = HBaseConfiguration
      .create()
    //连接hbase所在机器
    config.set("hbaseZookeeperQuorum", hbaseZookeeperQuorum)
    //建立连接
    val connection = ConnectionFactory.createConnection(config)
    //根据rowKey生成一个get,通过get得到表中数据
    val get: Get = new Get(Bytes.toBytes(rowKey))

    //指定查找的字段
    // 需要查询的用户标签,只需要查询规则条件中的标签即可,不需要全部查询
    val tags: Set[String] = userProfileConditions.keySet
    tags.foreach(tag => {
      get.addColumn(Bytes.toBytes(family),Bytes.toBytes(tag))
    })


    try{
      val table = connection.getTable(TableName.valueOf(tableName))
      //获取结果
      val result: Result = table.get(get)


      val sexBytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes("sex"))
      val ageBytes = result.getValue(Bytes.toBytes(family), Bytes.toBytes("age"))

      val querySexStr: String = Bytes.toString(sexBytes)
      val queryAgeStr: Int = Bytes.toInt(ageBytes)

      //比较性别
      val sexTp = userProfileConditions.getOrElse("sex", (EventRuleConstant.OPERATOR_EQUEAL, ""))
      //性别的比较方式
      val sexOpt = sexTp._1
      //规则中要求满足的值
      val sexValue = sexTp._2
      //真正的比较方法
      isMatch = compareUserTag(sexOpt, sexValue, querySexStr)

      //比较年龄
      val ageTp = userProfileConditions.getOrElse("age", (EventRuleConstant.OPERATOR_GREATERTHAN, ""))
      //年龄的比较方式
      val ageOpt = sexTp._1
      //规则中要求满足的值
      val ageValue = sexTp._2
      //真正的比较方法
      isMatch = compareUserTag(ageOpt, ageValue, querySexStr)


//      val cellScanner: CellScanner = result.cellScanner()
//
//      //判断hbase里是否还有数据
//      while (cellScanner.advance()){
//        val cell: Cell = cellScanner.current()
//
//
//        val columnName = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
//        val columnValue = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
//
//        println(s"columnName: ${columnName}, columnValue: ${columnValue}")
//      }

     /* result.rawCells().foreach(elem => {
        elem
      })*/

    }catch{
      case e: Exception =>println("程序异常")
    }

    //关闭连接
    connection.close()
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
      case EventRuleConstant.OPERATOR_LESSTHAN_EQUEAL => {
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
