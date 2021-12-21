package com.han.Test;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 测试字符串拼接时间消耗
 *
 * @author HanDongfang
 * @version 1.0
 * @create 2021-12-21  15:37
 */
public class StrBuilderDemo {
    List<String> list = new ArrayList<>();

    @Before
    public void init() {
        IntStream.range(0, 100000).forEach((index) -> {
            list.add("str" + index);
        });
    }

    @org.junit.Test
    public void test1() {
        String ss = "";
        long startTime = System.currentTimeMillis();
        for (String s : list) {
            ss += s;
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    @org.junit.Test
    public void test2() {
        String ss = "";
        long startTime = System.currentTimeMillis();
        for (String s : list) {
            ss = ss.concat(s);
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    @org.junit.Test
    public void test3() {
        StringBuilder ss = new StringBuilder();
        long startTime = System.currentTimeMillis();
        for (String s : list) {
            ss.append(s);
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }

    @org.junit.Test
    public void test4() {
        long startTime = System.currentTimeMillis();
        StringUtils.join(list);
        System.out.println(System.currentTimeMillis() - startTime);
    }

    @org.junit.Test
    public void test5() {
        StringBuffer ss = new StringBuffer();
        long startTime = System.currentTimeMillis();
        for (String s : list) {
            ss.append(s);
        }
        System.out.println(System.currentTimeMillis() - startTime);
    }
}
