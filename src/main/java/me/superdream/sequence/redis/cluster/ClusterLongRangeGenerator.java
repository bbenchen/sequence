/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis.cluster;

import me.superdream.sequence.redis.AbstractLongRangeGenerator;
import redis.clients.jedis.JedisCluster;

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
public class ClusterLongRangeGenerator extends AbstractLongRangeGenerator {
    private final JedisCluster jedisCluster; // Redis集群客户端实例

    public ClusterLongRangeGenerator(JedisCluster jedisCluster, String hname, String hkey, int step) {
        super(hname, hkey, step);
        this.jedisCluster = jedisCluster;
    }

    @Override
    protected void refreshMaxStepValue() {
        Long resNum = jedisCluster.hincrBy(hname, hkey, step);
        maxStepValue = resNum.intValue();
    }
}
