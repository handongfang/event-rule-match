package com.bigdata.rulematch.java.news.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * 跨界查询工具类
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-28  16:58
 */
public class CrossTimeQueryUtil {
    /**
     * 根据传入的查询时间点，获取一个查询分界点
     * 这个方法可以保证查询分界点不会一直变化，例如9:01到9:59得到的分界点都是8点
     * 问题是能用到的状态数据时间范围减小。。。
     *
     * @param queryTimeStamp
     */
    public static Long getBoundPoint(Long queryTimeStamp) {

        //时间按小时向上取整，比如9:15向上取整得到10点
        Date ceilDate = DateUtils.ceiling(new Date(queryTimeStamp), Calendar.HOUR);

        //再减去2小时
        long boundPointTime = DateUtils.addHours(ceilDate, -2).getTime();

        return boundPointTime;
    }
}
