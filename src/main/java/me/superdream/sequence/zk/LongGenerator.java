/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.zk;

import me.superdream.sequence.Generator;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.CachedAtomicLong;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * 基于ZooKeeper实现长整型序列号生成器
 * <p>
 *     支持缓存一定数量的序列号
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class LongGenerator implements Generator<Long> {
    private static final Logger log = LoggerFactory.getLogger(LongGenerator.class);
    private final CachedAtomicLong atomicLong;

    public LongGenerator(DistributedAtomicLong atomicLong, int cacheFactor) {
        checkNotNull(atomicLong, "atomicLong is null");

        this.atomicLong = new CachedAtomicLong(atomicLong, cacheFactor);
    }

    public LongGenerator(CachedAtomicLong atomicLong) {
        this.atomicLong = atomicLong;
    }

    @Override
    public Long next() {
        try {
            AtomicValue<Long> code = atomicLong.next();
            if (code.succeeded())
                return code.postValue();

            return -1L;
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Cannot get the increment serial number from ZooKeeper", ex);
            }

            return -1L;
        }
    }
}
