package com.bigdata.rulematch.scala.old.router

import java.sql.Connection

import com.bigdata.rulematch.scala.old.bean.EventLogBean
import com.bigdata.rulematch.scala.old.bean.rule.{EventCondition, EventSeqCondition, RuleCondition}
import com.bigdata.rulematch.scala.old.service.impl.{ClickHouseQueryServiceImpl, HBaseQueryServiceImpl, StateQueryServiceImpl}
import com.bigdata.rulematch.scala.old.utils.{ConnectionUtils, EventRuleCompareUtils, SegmentQueryUtil}
import org.apache.commons.dbutils.DbUtils
import org.apache.flink.api.common.state.ListState
import org.slf4j.{Logger, LoggerFactory}

/**
 * 支持分段的查询路由, 用于控制分段查询流程
 */
class SegmentQueryRouter {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  /**
   * hbase连接
   */
  private val hbaseConn: org.apache.hadoop.hbase.client.Connection = ConnectionUtils.getHBaseConnection()
  /**
   * clickhouse连接
   */
  private val ckConn: Connection = ConnectionUtils.getClickHouseConnection()

  //初始化hbase查询服务对象
  private val hBaseQueryService: HBaseQueryServiceImpl = new HBaseQueryServiceImpl(hbaseConn)

  //初始化clickhouse查询服务对象
  private val clickHouseQueryService: ClickHouseQueryServiceImpl = new ClickHouseQueryServiceImpl(ckConn)

  //初始化flink state查询服务对象
  private val stateQueryService: StateQueryServiceImpl = new StateQueryServiceImpl()

  /**
   * 规则匹配
   */
  def ruleMatch(event: EventLogBean,
                keyByFiedValue: String,
                ruleCondition: RuleCondition,
                eventListState: ListState[EventLogBean]) = {
    var isMatch = false

    //判断是否满足规则触发条件
    if (EventRuleCompareUtils.eventMatchCondition(event, ruleCondition.triggerEventCondition)) {
      logger.debug(s"满足规则的触发条件: ${ruleCondition.triggerEventCondition}")

      //满足规则的触发条件,才继续进行其他规则条件的匹配

      isMatch = true

      //判断是否满足用户画像条件（hbase）
      val userProfileConditions: Map[String, (String, String)] = ruleCondition.userProfileConditions

      if (userProfileConditions != null && userProfileConditions.size > 0) {

        //只有设置了用户画像类条件,才去匹配
        logger.debug(s"开始匹配用户画像类规则条件: ${userProfileConditions}")

        //从hbase中查询，并判断是否匹配
        isMatch = hBaseQueryService.userProfileConditionIsMatch(event.userId, userProfileConditions)

      } else {
        logger.debug("没有设置用户画像规则类条件")
      }

      if (isMatch) {
        logger.debug("用户画像类规则满足,开始匹配行为次数类条件 ")

        //根据当前的查询时间（也可以根据事件中的时间）,获取查询分界点
        val queryBoundPoint: Long = SegmentQueryUtil.getBoundPoint(System.currentTimeMillis())

        //行为次数类条件
        val actionCountConditionList = ruleCondition.actionCountConditionList
        if (actionCountConditionList != null && actionCountConditionList.size > 0) {

          logger.debug(s"开始匹配行为次数类规则条件: ${actionCountConditionList}")

          val actionCountIterator = actionCountConditionList.iterator

          while (actionCountIterator.hasNext && isMatch) {
            val countCondition: EventCondition = actionCountIterator.next()

            //开始根据查询分界点, 进行分段查询
            if (countCondition.timeRangeStart >= queryBoundPoint) {
              //如果规则的开始时间都在分界点的右侧,则只需要查询flink state
              //                     |-------------
              //-------------------|------------------
              val matchCount = stateQueryService.stateQueryEventCount(eventListState, countCondition, countCondition.timeRangeStart,
                countCondition.timeRangeEnd)

              if (matchCount < countCondition.minLimit || matchCount > countCondition.maxLimit) {
                isMatch = false
              }

            } else if (countCondition.timeRangeEnd < queryBoundPoint) {
              //如果规则的结束时间都在分界点的左侧,则只需要查询clickhouse
              // -------------|
              //-------------------|------------------
              val queryCnt = clickHouseQueryService.queryActionCountCondition(ruleCondition.keyByFields, keyByFiedValue, countCondition)
              //拿查询出来的行为次数与规则中要求的规则次数进行比较
              if (queryCnt < countCondition.minLimit || queryCnt > countCondition.maxLimit) {
                isMatch = false
              }

            } else {
              //跨界查询
              //            |-------------|
              //-------------------|------------------
              //先查flink state (分界点往后的查询state)
              val matchCountInState = stateQueryService.stateQueryEventCount(eventListState, countCondition, queryBoundPoint, countCondition.timeRangeEnd)

              if (matchCountInState < countCondition.minLimit || matchCountInState > countCondition.maxLimit) {
                //state中不满足,再查clickhouse  (分界点往前的查询state)
                val matchCountInCK = clickHouseQueryService.queryActionCountCondition(ruleCondition.keyByFields,
                  keyByFiedValue, countCondition, countCondition.timeRangeStart, queryBoundPoint)

                val matchCount = matchCountInState + matchCountInCK
                if (matchCount < countCondition.minLimit || matchCount > countCondition.maxLimit) {
                  isMatch = false
                }

              }
            }

          }

        } else {
          logger.debug("没有设置行为次数类规则条件")
        }

        //行为次序类条件  （clickhouse）
        if (isMatch) {

          logger.debug("行为次数类规则满足,开始匹配行为次序类条件 ")

          val actionSeqConditionList = ruleCondition.actionSeqConditionList
          if (actionSeqConditionList != null && actionSeqConditionList.size > 0) {
            logger.debug(s"开始匹配行为次序类规则条件: ${actionSeqConditionList}")

            val actionSeqIterator = actionSeqConditionList.iterator

            while (actionSeqIterator.hasNext && isMatch) {
              val seqCondition: EventSeqCondition = actionSeqIterator.next()

              if (seqCondition.timeRangeStart >= queryBoundPoint) {
                //规则的起始时间大于分界点, 只查state
                val maxStep = stateQueryService.stateQueryEventSequence(eventListState, seqCondition.eventSeqList,
                  seqCondition.timeRangeStart, seqCondition.timeRangeEnd)

                //拿查询出来的最大匹配步骤与规则中要求的次序条件个数进行比较
                if (maxStep < seqCondition.eventSeqList.size) {
                  isMatch = false
                }

              } else if (seqCondition.timeRangeEnd < queryBoundPoint) {
                //规则的结束时间小于分界点, 只查ck
                //查询最大匹配步骤
                val maxStep = clickHouseQueryService.queryActionSeqCondition(ruleCondition.keyByFields, keyByFiedValue, seqCondition)
                //拿查询出来的最大匹配步骤与规则中要求的次序条件个数进行比较
                if (maxStep < seqCondition.eventSeqList.size) {
                  isMatch = false
                }

              } else {
                //跨界查询
                val eventSeqList = seqCondition.eventSeqList
                //先查一次state,有可能在state中就直接满足了,就不需要再查询clickhouse了
                var maxStep = stateQueryService.stateQueryEventSequence(eventListState, eventSeqList,
                  queryBoundPoint, seqCondition.timeRangeEnd)

                if (maxStep < eventSeqList.size) {
                  //如果状态中没有满足,再分别查询 ck 和 state, state中的数据没有复用,是因为需要考虑的情况比较复杂
                  //查询ck
                  val ckMaxStep = clickHouseQueryService.queryActionSeqCondition(ruleCondition.keyByFields,
                    keyByFiedValue, seqCondition, seqCondition.timeRangeStart, queryBoundPoint)

                  if (ckMaxStep < eventSeqList.size) {
                    //当ck中查询到的还不满足条件时，才需要再次查询state
                    //把序列条件截短
                    val truncateSeqList = eventSeqList.slice(ckMaxStep, eventSeqList.size)
                    val stateMaxStep = stateQueryService.stateQueryEventSequence(eventListState, truncateSeqList,
                      queryBoundPoint, seqCondition.timeRangeEnd)

                    maxStep = ckMaxStep + stateMaxStep
                    if (maxStep < eventSeqList.size) {
                      //如果一个seq条件不满足,整个循环都可以跳出了
                      isMatch = false
                    }

                  }

                }

              }

            }

          } else {
            logger.debug("没有设置行为次序类规则条件")
          }

        } else {
          logger.debug("存在不满足的规则条件, 行为次序类规则不需要进行匹配 ")
        }

      }

    } else {
      logger.debug(s"不满足规则的触发条件: ${ruleCondition.triggerEventCondition}")
    }

    isMatch
  }

  /**
   * 关闭连接
   */
  def closeConnection() = {
    //关闭hbase连接
    if (hbaseConn != null) {
      try {
        hbaseConn.close()
      } catch {
        case _ =>
      }
    }

    //关闭clickhouse连接
    DbUtils.closeQuietly(ckConn)
  }
}
