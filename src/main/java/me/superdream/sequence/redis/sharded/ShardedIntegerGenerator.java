/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.sharded;

import me.superdream.sequence.redis.AbstractGenerator;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

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
public class ShardedIntegerGenerator extends AbstractGenerator<Integer> {
    private final ShardedJedisPool shardedJedisPool; // Redis的分片客户端实例

    public ShardedIntegerGenerator(ShardedJedisPool shardedJedisPool, String hname, String hkey) {
        super(hname, hkey);
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    public Integer next() {
        try (ShardedJedis jedis = shardedJedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, 1L);
            return resNum == null ? -1 : resNum.intValue();
        }
    }
}
