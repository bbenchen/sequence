/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.zk;

import me.superdream.sequence.Generator;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * 基于ZooKeeper的序列生成测试
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class ZKGeneratorTest {
    private static TestingServer server;
    private static CuratorFramework curator;

    @BeforeClass
    public static void beforeAll() throws Exception {
        server = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, Integer.MAX_VALUE));
        curator.start();
    }

    @AfterClass
    public static void afterAll() throws Exception {
        CloseableUtils.closeQuietly(curator);
        CloseableUtils.closeQuietly(server);
    }

    @Test
    public void testIntegerGenerator() {
        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(curator, "/code/int", new ExponentialBackoffRetry(1000, 10));
        Generator<Integer> generator = new IntegerGenerator(atomicInteger, 10);
        for (int i = 0; i < 100; i++) {
            int seqId = generator.next();
            assertNotEquals(seqId, -1);
        }
    }

    @Test
    public void testIntegerRangeGenerator() {
        Generator<Integer> generator = new IntegerRangeGenerator(curator, "/code/int/range", 10000, 40);
        List<Integer> seqIds = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            int seqId = generator.next();
            seqIds.add(seqId);
            assertNotEquals(seqId, -1);
        }
        System.out.println(seqIds.stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testLongGenerator() {
        DistributedAtomicLong atomicLong = new DistributedAtomicLong(curator, "/code/int", new ExponentialBackoffRetry(1000, 10));
        Generator<Long> generator = new LongGenerator(atomicLong, 10);
        for (int i = 0; i < 100; i++) {
            long seqId = generator.next();
            assertNotEquals(seqId, -1L);
        }
    }

    @Test
    public void testLongRangeGenerator() {
        Generator<Long> generator = new LongRangeGenerator(curator, "/code/long/range", 10000, 40);
        List<Long> seqIds = new LinkedList<>();
        for (int i = 0; i < 100; i++) {
            long seqId = generator.next();
            seqIds.add(seqId);
            assertNotEquals(seqId, -1);
        }
        System.out.println(seqIds.stream().sorted().collect(Collectors.toList()));
    }
}