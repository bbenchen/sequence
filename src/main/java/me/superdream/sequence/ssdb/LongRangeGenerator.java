/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.ssdb;

import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.*;

/**
 * 基于指定范围随机获取的长整形对序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class LongRangeGenerator extends AbstractGenerator<Long> {
    private final int step; // 步长

    private final long values[];
    private final long excludes[];

    public LongRangeGenerator(SSDB ssdb, String hname, String hkey, int step) {
        super(ssdb, hname, hkey);
        this.step = step;

        this.values = new long[step];
        this.excludes = new long[step];
        fill(excludes, -1L);

        createRange(step);
    }

    @Override
    public Long next() {
        synchronized (this) {
            long id = -1L;
            for (int i = 0; i < values.length; i++) {
                long excludeIndex = excludes[i];
                if (excludeIndex == -1L) {
                    excludes[i] = i;
                    id = values[i];
                    break;
                }
            }

            checkFinish();

            return id;
        }
    }

    private void createRange(int step) {
        Random random = ThreadLocalRandom.current();

        Response res = ssdb.hincr(hname, hkey, step);
        long min = res.asLong() - step;

        for (int i = 0; i < step; i++) {
            values[i] = min + i;
        }

        for (int i = 0; i < step; i++) {
            int temp1 = random.nextInt(step); //随机产生一个位置
            int temp2 = random.nextInt(step); //随机产生另一个位置

            if (temp1 != temp2) {
                long temp3 = values[temp1];
                values[temp1] = values[temp2];
                values[temp2] = temp3;
            }
        }
    }

    private void checkFinish() {
        long[] cs = excludes.clone();
        sort(cs);
        int index = binarySearch(cs, -1L);
        if (index < 0) {
            fill(excludes, -1L);
            createRange(step);
        }
    }
}
