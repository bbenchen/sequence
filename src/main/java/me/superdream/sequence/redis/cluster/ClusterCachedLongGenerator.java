/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.cluster;

import me.superdream.sequence.redis.AbstractCachedLongGenerator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * 基于Redis普通连接池和Sentinel连接池实现的带缓长存整形序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class ClusterCachedLongGenerator extends AbstractCachedLongGenerator {
    private final JedisCluster jedisCluster; // Redis集群客户端实例

    public ClusterCachedLongGenerator(JedisCluster jedisCluster, String hname, String hkey, int cachedNum) {
        super(hname, hkey, cachedNum);
        this.jedisCluster = jedisCluster;
    }

    @Override
    protected void refreshMaxSeqId() {
        Long resNum = jedisCluster.hincrBy(hname, hkey, cachedNum);
        if (resNum != null)
            maxSeqId = resNum;
    }
}
