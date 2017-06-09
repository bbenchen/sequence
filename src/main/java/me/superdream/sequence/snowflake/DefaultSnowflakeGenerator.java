/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于Twitter Snowflake算法的默认序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class DefaultSnowflakeGenerator implements SnowflakeGenerator {
    private static final Logger log = LoggerFactory.getLogger(DefaultSnowflakeGenerator.class);

    private final long epoch; // 初始纪元时间
    private long workerId;  //数据中心
    private long machineId;     //机器标识
    private long sequence = 0L; //序列号
    private long lastTimestamp = -1L;//上一次时间戳

    public DefaultSnowflakeGenerator(long machineId, long workerId) {
        this(EPOCH, machineId, workerId);
    }

    public DefaultSnowflakeGenerator(long epoch, long machineId, long workerId) {
        if (epoch >= System.currentTimeMillis()) {
            throw new IllegalArgumentException("epoch can't be greater than current time");
        }
        if (workerId > MAX_WORKER_NUM || workerId < 0) {
            throw new IllegalArgumentException("workerId can't be greater than MAX_WORKER_NUM or less than 0");
        }
        if (machineId > MAX_MACHINE_NUM || machineId < 0) {
            throw new IllegalArgumentException("machineId can't be greater than MAX_MACHINE_NUM or less than 0");
        }
        this.epoch = epoch;
        this.workerId = workerId;
        this.machineId = machineId;
    }

    @Override
    public long getEpoch() {
        return epoch;
    }

    private long getNextTimestamp() {
        long mill = getCurrTimestamp();
        while (mill <= lastTimestamp) {
            mill = getCurrTimestamp();
        }
        return mill;
    }

    private long getCurrTimestamp() {
        return System.currentTimeMillis();
    }

    /**
     * 产生下一个ID
     */
    public Long next() {
        synchronized (this) {
            long currTimeStamp = getCurrTimestamp();
            if (currTimeStamp < lastTimestamp) {
                if (log.isWarnEnabled()) {
                    log.warn("Clock moved backwards. Refusing to generate id");
                }
                return -1L;
            }

            if (currTimeStamp == lastTimestamp) {
                //相同毫秒内，序列号自增
                sequence = (sequence + 1) & MAX_SEQUENCE;
                //同一毫秒的序列数已经达到最大
                if (sequence == 0L) {
                    currTimeStamp = getNextTimestamp();
                }
            } else {
                //不同毫秒内，序列号置为0
                sequence = 0L;
            }

            lastTimestamp = currTimeStamp;

            return (currTimeStamp - epoch) << TIMESTAMP_LEFT //时间戳部分
                    | workerId << WORKER_LEFT       //数据中心部分
                    | machineId << MACHINE_LEFT             //机器标识部分
                    | sequence;                             //序列号部分
        }
    }
}
