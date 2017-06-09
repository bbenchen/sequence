/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.sharded;

import me.superdream.sequence.redis.AbstractCachedLongGenerator;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的带缓长存整形序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class ShardedCachedLongGenerator extends AbstractCachedLongGenerator {
    private final ShardedJedisPool shardedJedisPool; // Redis的分片客户端实例

    public ShardedCachedLongGenerator(ShardedJedisPool shardedJedisPool, String hname, String hkey, int cachedNum) {
        super(hname, hkey, cachedNum);
        this.shardedJedisPool = shardedJedisPool;
    }

    @Override
    protected void refreshMaxSeqId() {
        try (ShardedJedis jedis = shardedJedisPool.getResource()) {
            Long resNum = jedis.hincrBy(hname, hkey, cachedNum);
            if (resNum != null)
                maxSeqId = resNum;
        }
    }
}
