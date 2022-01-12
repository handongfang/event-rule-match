package com.bigdata.rulematch.scala.news.buffer

import java.util

import com.bigdata.rulematch.scala.news.beans.BufferData
import com.bigdata.rulematch.scala.news.utils.ConnectionUtils
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * 缓存管理器接口的redis实现类
 */
class BufferManagerImpl extends BufferManager {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  private val connection: StatefulRedisConnection[String, String] = ConnectionUtils.getRedisCommands()
  private val redisCommands: RedisCommands[String, String] = connection.sync()

  override def getDataFromBuffer(bufferKey: String): BufferData = {

    val valueMap: util.Map[String, String] = redisCommands.hgetall(bufferKey)

    //bufferKey的格式: keyByValue+":"+cacheId
    val fields = bufferKey.split(":")

    val userId = fields(0)
    val cacheId = fields(1)

    BufferData(userId, cacheId, valueMap)
  }

  override def putDataToBuffer(bufferData: BufferData): Boolean = {
    //hset中传map需要redis服务端支持,如果服务端版本过低,这样写会报错
    //val addCount = redisCommands.hset(bufferData.getCacheKey(), bufferData.valueMap)
    val replyStr = redisCommands.hmset(bufferData.getCacheKey(), bufferData.valueMap)
    if (StringUtils.equals("OK", replyStr)) true else false
  }

  override def putDataToBuffer(bufferKey: String, valueMap: Map[String, String]): Boolean = {

    val javaValueMap = scala.collection.JavaConverters.mapAsJavaMap(valueMap)
    //hset中传map需要redis服务端支持,如果服务端版本过低,这样写会报错
    //val addCount = redisCommands.hset(bufferKey, javaValueMap)
    val replyStr = redisCommands.hset(bufferKey, javaValueMap)
    if (StringUtils.equals("OK", replyStr)) true else false
  }

  override def delBufferEntry(bufferKey: String, key: String) = {
    redisCommands.hdel(bufferKey, key)
  }

  override def close(): Unit = {
    if (connection != null) {
      try {
        connection.close()
      } catch {
        case _ =>
      }
    }
  }
}
