package com.bigdata.rulematch.scala.news.buffer

import com.bigdata.rulematch.scala.news.beans.BufferData

/**
 * 缓存管理器接口
 */
trait BufferManager {
  /**
   * 从缓存中获取数据
   * @param bufferKey
   * @return
   */
  def getDataFromBuffer(bufferKey: String): BufferData

  /**
   * 给定一个缓存对象，放入缓存中
   * @param bufferData
   * @return
   */
  def putDataToBuffer(bufferData: BufferData): Boolean

  /**
   * 给定一个bufferKey和一个valueMap，放入缓存中
   * @param bufferKey
   * @param valueMap
   * @return
   */
  def putDataToBuffer(bufferKey: String, valueMap: Map[String, String]): Boolean

  /**
   * 根据指定的 bufferKey 和 key, 删除缓存
   * @param bufferKey
   * @param key
   * @return
   */
  def delBufferEntry(bufferKey: String, key: String)

  /**
   * 关闭连接的方法（如果使用了外部连接）
   */
  def close()
}
