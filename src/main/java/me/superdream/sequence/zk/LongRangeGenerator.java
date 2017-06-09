/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.zk;

import me.superdream.sequence.Generator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.PromotedToLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.*;
import static java.util.Arrays.*;

/**
 * 基于ZooKeeper实现带范围的整型序列号生成器
 * <p>
 *     每次都随机从该范围内获取序列号
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class LongRangeGenerator implements Generator<Long> {
    private static final Logger logger = LoggerFactory.getLogger(LongRangeGenerator.class);
    private final CuratorFramework curator;
    private final DistributedAtomicLong atomicLong;

    private final String path;
    private final long values[];
    private final long excludes[];

    public LongRangeGenerator(CuratorFramework curator, String path, long initNum, int step) {
        this(curator, path, null, initNum, step);
    }

    public LongRangeGenerator(CuratorFramework curator, String path, RetryPolicy retryPolicy, long initNum, int step) {
        checkNotNull(curator, "curator is null");
        checkArgument(path != null && !path.isEmpty(), "path is null or empty");
        checkArgument(initNum > 0, "initNum less than or equal to 0");
        checkArgument(step > 0, "step less than 10");

        checkState(curator.getState() == CuratorFrameworkState.STARTED, "instance is not started.");

        if (path.endsWith("/"))
            path = path.substring(0, path.length() - 1);
        if (!path.startsWith("/"))
            path = "/" + path;

        if (retryPolicy == null)
            retryPolicy = getRetryPolicy();

        this.curator = curator;
        this.path = path;
        this.atomicLong = new DistributedAtomicLong(curator, path, retryPolicy, getPromotedToLock());

        this.values = new long[step];
        this.excludes = new long[step];
        fill(excludes, -1);

        init(initNum, step);
    }

    private RetryPolicy getRetryPolicy() {
        return new ExponentialBackoffRetry(1000, Integer.MAX_VALUE);
    }

    private PromotedToLock getPromotedToLock() {
        return PromotedToLock.builder()
                .lockPath(path + "/lock")
                .timeout(1, TimeUnit.SECONDS)
                .retryPolicy(getRetryPolicy())
                .build();
    }

    private void init(long initNum, int step) {
        try {
            long min = initNum;
            if (curator.checkExists().forPath(path) == null) {
                atomicLong.forceSet(initNum);
            } else {
                AtomicValue<Long> value = atomicLong.get();
                if (min > value.postValue()) {
                    atomicLong.forceSet(min);
                } else {
                    min = value.postValue();
                }
            }

            createRange(min, step);
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error(e.getMessage(), e);
        }
    }

    private void createRange(int step) {
        try {
            AtomicValue<Long> value = atomicLong.get();

            createRange(value.postValue(), step);
        } catch (Exception e) {
            if (logger.isErrorEnabled())
                logger.error(e.getMessage(), e);
        }
    }

    private void createRange(long min, int step) throws Exception {
        Random random = ThreadLocalRandom.current();
        AtomicValue<Long> curr;

        do {
            curr = atomicLong.compareAndSet(min, min + step);
            if (curr.succeeded()) {
                for (int i = 0; i < step; i++) {
                    values[i] = min + i;
                }

                for (int i = 0; i < step; i++) {
                    int temp1 = random.nextInt(step); //随机产生一个位置
                    int temp2 = random.nextInt(step); //随机产生另一个位置

                    if (temp1 != temp2) {
                        long temp3 = values[temp1];
                        values[temp1] = values[temp2];
                        values[temp2] = temp3;
                    }
                }
            } else {
                min = curr.postValue();
            }
        } while (!curr.succeeded());
    }

    private void checkFinish() {
        long[] cs = excludes.clone();
        sort(cs);
        int index = binarySearch(cs, -1);
        if (index < 0) {
            fill(excludes, -1);
            createRange(values.length);
        }
    }

    @Override
    public Long next() {
        synchronized (this) {
            long id = -1L;
            for (int i = 0; i < values.length; i++) {
                long excludeIndex = excludes[i];
                if (excludeIndex == -1L) {
                    excludes[i] = i;
                    id = values[i];
                    break;
                }
            }

            checkFinish();

            return id;
        }
    }
}
