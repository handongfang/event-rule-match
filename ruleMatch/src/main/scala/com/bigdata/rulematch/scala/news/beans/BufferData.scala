package com.bigdata.rulematch.scala.news.beans

import scala.beans.BeanProperty

/**
 * 用于封装缓存数据的对象
 */
case class BufferData(
                       /**
                        * 规则条件分组计算中的分组标识（如userId,或ip等）
                        */
                       @BeanProperty keyByValue: String,

                       /**
                        * 缓存id，用于唯一确定一个“规则条件”（事件约束相同）
                        */
                       @BeanProperty cacheId: String,

                       /**
                        * 缓存数据值（时间范围->事件序列）
                        */
                       @BeanProperty valueMap: java.util.Map[String, String]
                     ) {
  /**
   * 用于返回 拼接好的缓存顶层key
   *
   * @return
   */
  def getCacheKey() = {
    s"${this.keyByValue}:${this.cacheId}"
  }
}
