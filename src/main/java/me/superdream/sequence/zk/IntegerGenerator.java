/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.zk;

import me.superdream.sequence.Generator;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.CachedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 基于ZooKeeper实现整型序列号生成器
 * <p>
 *     支持缓存一定数量的序列号
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class IntegerGenerator implements Generator<Integer> {
    private static final Logger log = LoggerFactory.getLogger(IntegerGenerator.class);
    private final CachedAtomicInteger atomicInteger;

    public IntegerGenerator(DistributedAtomicInteger atomicInteger, int cacheFactor) {
        checkNotNull(atomicInteger, "atomicInteger is null");

        this.atomicInteger = new CachedAtomicInteger(atomicInteger, cacheFactor);
    }

    public IntegerGenerator(CachedAtomicInteger atomicInteger) {
        this.atomicInteger = atomicInteger;
    }

    @Override
    public Integer next() {
        try {
            AtomicValue<Integer> code = atomicInteger.next();
            if (code.succeeded())
                return code.postValue();

            return -1;
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Cannot get the increment serial number from ZooKeeper", ex);
            }

            return -1;
        }
    }
}
