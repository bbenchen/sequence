/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.generic;

import me.superdream.sequence.redis.AbstractCachedIntegerGenerator;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的带缓存整形序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class GenericCachedIntegerGenerator extends AbstractCachedIntegerGenerator {
    private final Pool<Jedis> jedisPool;

    public GenericCachedIntegerGenerator(Pool<Jedis> jedisPool, String hname, String hkey, int cachedNum) {
        super(hname, hkey, cachedNum);
        this.jedisPool = jedisPool;
    }

    @Override
    protected void refreshMaxSeqId() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, cachedNum);
            if (resNum != null)
                maxSeqId = resNum.intValue();
        }
    }
}
