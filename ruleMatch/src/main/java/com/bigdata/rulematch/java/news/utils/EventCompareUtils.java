package com.bigdata.rulematch.java.news.utils;

/**
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  11:25
 */

import com.bigdata.rulematch.java.news.beans.EventLogBean;
import com.bigdata.rulematch.java.news.beans.rule.EventCondition;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 事件比较的工具类
 */
public class EventCompareUtils {
    /**
     * 传入的事件序列中匹配给定模式,并返回匹配次数
     * 用作组合条件中次数类条件与次序类条件的匹配
     *
     * @param eventStr
     * @param matchPattern
     * @return
     */
    public static int sequenceStrMatchRegexCount(String eventStr, String matchPattern) {
        //匹配正则表达式，得到匹配的次数
        Pattern pattern = Pattern.compile(matchPattern);

        Matcher matcher = pattern.matcher(eventStr);

        int matchCount = 0;
        while (matcher.find()) {
            matchCount += 1;
        }

        return matchCount;
    }

    /**
     * 比较给定的事件是否与事件原子条件匹配，匹配触发条件的时候,会使用这个方法
     *
     * @param event
     * @param eventCondition
     * @return
     */
    public static boolean eventMatchCondition(EventLogBean event, EventCondition eventCondition) {
        boolean isMatch = false;
        if (StringUtils.equals(event.getEventId(), eventCondition.getEventId())) {
            isMatch = true;
            //事件ID相同，还需要判断事件属性是否满足（可以不用比较url）
            Set<String> keysSet = eventCondition.getEventProps().keySet();
            Iterator<String> keysIterator = keysSet.iterator();
            while (keysIterator.hasNext()) {
                String key = keysIterator.next();
                //规则条件中这个key对应的value
                String conditionValue = eventCondition.getEventProps().getOrDefault(key, "");

                if (!StringUtils.equals(conditionValue, event.getProperties().get(key))) {
                    //如果事件中某个属性与规则条件中相同key对应的属性值不同,则说明不匹配,需要跳出
                    isMatch = false;
                }
            }

        }

        //返回是否匹配
        return isMatch;
    }
}
