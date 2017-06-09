/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.sharded;

import me.superdream.sequence.redis.AbstractLongRangeGenerator;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的带范围整形序列生成器
 * <p>
 *     第次都随机从该范围内获取序列号
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class ShardedLongRangeGenerator extends AbstractLongRangeGenerator {
    private final ShardedJedisPool shardedJedisPool; // Redis的分片客户端实例

    public ShardedLongRangeGenerator(ShardedJedisPool shardedJedisPool, String hname, String hkey, int step) {
        super(hname, hkey, step);
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    protected void refreshMaxStepValue() {
        try (ShardedJedis jedis = shardedJedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, step);
            maxStepValue = resNum.intValue();
        }
    }
}
