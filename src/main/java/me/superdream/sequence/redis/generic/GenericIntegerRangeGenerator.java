/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.generic;

import me.superdream.sequence.redis.AbstractIntegerRangeGenerator;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的带范围整形序列生成器
 * <p>
 *     每次都随机从该范围内获取序列号
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class GenericIntegerRangeGenerator extends AbstractIntegerRangeGenerator {
    private final Pool<Jedis> jedisPool;

    public GenericIntegerRangeGenerator(Pool<Jedis> jedisPool, String hname, String hkey, int step) {
        super(hname, hkey, step);
        this.jedisPool = jedisPool;
    }

    @Override
    protected void refreshMaxStepValue() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, step);
            maxStepValue = resNum.intValue();
        }
    }
}
