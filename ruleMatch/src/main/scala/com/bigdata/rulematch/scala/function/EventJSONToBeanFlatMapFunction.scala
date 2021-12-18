package com.bigdata.rulematch.scala.function

import com.alibaba.fastjson.JSON
import com.bigdata.rulematch.scala.bean.EventLogBean
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/**
 *
 * 把event事件的json字符串格式转换为一个EventLogBean对象
 *
 * @author Administrator
 * @version 1.0
 * @date 2021-12-18 19:23
 */
class EventJSONToBeanFlatMapFunction extends RichFlatMapFunction[String, EventLogBean] {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  override def flatMap(value: String, out: Collector[EventLogBean]): Unit = {

    try {
      //将JSON 数据变成 EventLogBean
      val eventLogBean = JSON.parseObject(value, classOf[EventLogBean])

      out.collect(eventLogBean)
    } catch {
      case e: Throwable => {
        logger.error(s"JSON解析异常: ${value}", e)
      }
    }


  }
}
