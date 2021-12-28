package com.bigdata.rulematch.scala.datagen

import com.bigdata.rulematch.scala.old.conf.EventRuleConstant
import com.bigdata.rulematch.scala.old.utils.ConnectionUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
 *
 * 模拟用户画像数据生成,并插入HBase
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-20 19:11
 */
object UserProfileDataMock {
  def main(args: Array[String]): Unit = {

    //获取HBase连接对象
    val connection: Connection = ConnectionUtils.getHBaseConnection()

    val table = connection.getTable(TableName.valueOf(EventRuleConstant.HBASE_USER_PROFILE_TABLE_NAME))

    val userId = "u202112180001"

    val rowkey = userId

    val put = new Put(Bytes.toBytes(rowkey))

    val family = Bytes.toBytes("f")

    put.addColumn(family, Bytes.toBytes("sex"), Bytes.toBytes("male"))
    put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes("18"))

    //插入数据
    table.put(put)

    // 关闭连接
    table.close()
    connection.close()

  }
}
