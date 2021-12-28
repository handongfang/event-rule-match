package com.bigdata.rulematch.scala.news.utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

/**
 * 跨界查询工具类
 */
object CrossTimeQueryUtil {

  /**
   * 根据传入的查询时间点，获取一个查询分界点
   *
   * 这个方法可以保证查询分界点不会一直变化，例如9:01到9:59得到的分界点都是8点
   *
   * @param queryTimeStamp
   */
  def getBoundPoint(queryTimeStamp: Long) = {

    //时间按小时向上取整，比如9:15向上取整得到10点
    val ceilDate: Date = DateUtils.ceiling(new Date(queryTimeStamp), Calendar.HOUR)

    //再减去2小时
    DateUtils.addHours(ceilDate, -2).getTime

  }

  def main(args: Array[String]): Unit = {
    val dateStr = "2021-12-25 09:15:18"

    val queryTimeStamp = DateUtils.parseDate(dateStr, "yyyy-MM-dd HH:mm:ss").getTime

    val boundPointTimeStamp = getBoundPoint(queryTimeStamp)

    val boundPointTimeStr = DateFormatUtils.format(boundPointTimeStamp, "yyyy-MM-dd HH:mm:ss")

    println(boundPointTimeStr)
  }
}
