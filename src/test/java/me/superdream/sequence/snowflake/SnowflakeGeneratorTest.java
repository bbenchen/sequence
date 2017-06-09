/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.snowflake;

import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * Twitter Snowflake算法测试
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class SnowflakeGeneratorTest {
    @Test
    public void testNextAndParse() throws Exception {
        SnowflakeGenerator snowflake = new DefaultSnowflakeGenerator(2, 1);
        Long id = snowflake.next();
        System.out.println(id);
        System.out.println(snowflake.parse(id));
        String bitStr = "111111111111111111111111111111111111111111";
        System.out.println(bitStr.length());
        System.out.println(Long.parseLong(bitStr, 2));
        System.out.println(new Date(Long.parseLong(bitStr, 2) + 1483200000000L));
    }
}