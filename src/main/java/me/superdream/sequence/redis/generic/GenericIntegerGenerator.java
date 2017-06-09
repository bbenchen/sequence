/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.generic;

import me.superdream.sequence.redis.AbstractGenerator;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的整形序列生成器
 * <p>
 *     每次获取序列号都需要与redis进行通信
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class GenericIntegerGenerator extends AbstractGenerator<Integer> {
    private final Pool<Jedis> jedisPool;

    public GenericIntegerGenerator(Pool<Jedis> jedisPool, String hname, String hkey) {
        super(hname, hkey);
        this.jedisPool = jedisPool;
    }

    @Override
    public Integer next() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, 1L);
            return resNum == null ? -1 : resNum.intValue();
        }
    }
}
