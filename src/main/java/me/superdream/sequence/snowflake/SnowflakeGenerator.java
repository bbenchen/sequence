/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.snowflake;

import me.superdream.sequence.Generator;

/**
 * 基于Twitter Snowflake算法的序列生成器
 * <p>
 * 64位ID (42(毫秒)+5(机器ID)+5(业务编码)+12(重复累加))
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public interface SnowflakeGenerator extends Generator<Long> {
    /**
     * 初始的时间戳（2017-01-01 00:00:00）
     */
    long EPOCH = 1483200000000L;

    /**
     * 每一部分占用的位数
     */
    long SEQUENCE_BIT = 12; //序列号占用的位数
    long MACHINE_BIT = 5; //机器标识占用的位数
    long WORKER_BIT = 5;//数据中心占用的位数

    /**
     * 每一部分的最大值
     */
    long MAX_WORKER_NUM = ~(-1L << WORKER_BIT);
    long MAX_MACHINE_NUM = ~(-1L << MACHINE_BIT);
    long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);

    /**
     * 每一部分向左的位移
     */
    long MACHINE_LEFT = SEQUENCE_BIT;
    long WORKER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
    long TIMESTAMP_LEFT = WORKER_LEFT + WORKER_BIT;

    /**
     * 解析Twitter Snowflake算法生成唯一编号
     *
     * @param id Twitter Snowflake算法生成唯一编号
     * @return 返回解析后的JSON字符串
     */
    default String parse(long id) {
        long sequence = (id << (64 - SEQUENCE_BIT)) >>> (64 - SEQUENCE_BIT);
        long machineId = (id << (42 + WORKER_BIT)) >>> (64 - MACHINE_BIT);
        long workerId = (id << 42) >>> (64 - WORKER_BIT);
        long deltaMillis = id >>> (WORKER_BIT + MACHINE_BIT + SEQUENCE_BIT);

        return String.format("{\"id\":\"%d\",\"timestamp\":\"%s\",\"machineId\":\"%d\",\"workerId\":\"%d\",\"sequence\":\"%d\"}",
                id, getEpoch() + deltaMillis, machineId, workerId, sequence);
    }

    /**
     * 获取纪元初始时间
     *
     * @return
     */
    long getEpoch();
}
