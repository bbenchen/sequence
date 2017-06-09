/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.ssdb;

import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.binarySearch;
import static java.util.Arrays.fill;
import static java.util.Arrays.sort;

/**
 * 基于指定范围随机获取的整形对序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class IntegerRangeGenerator extends AbstractGenerator<Integer> {
    private final int step; // 步长

    private final int values[];
    private final int excludes[];

    public IntegerRangeGenerator(SSDB ssdb, String hname, String hkey, int step) {
        super(ssdb, hname, hkey);
        this.step = step;

        this.values = new int[step];
        this.excludes = new int[step];
        fill(excludes, -1);

        createRange(step);
    }

    @Override
    public Integer next() {
        synchronized (this) {
            int id = -1;
            for (int i = 0; i < values.length; i++) {
                int excludeIndex = excludes[i];
                if (excludeIndex == -1) {
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
        int min = res.asInt() - step;

        for (int i = 0; i < step; i++) {
            values[i] = min + i;
        }

        for (int i = 0; i < step; i++) {
            int temp1 = random.nextInt(step); //随机产生一个位置
            int temp2 = random.nextInt(step); //随机产生另一个位置

            if (temp1 != temp2) {
                int temp3 = values[temp1];
                values[temp1] = values[temp2];
                values[temp2] = temp3;
            }
        }
    }

    private void checkFinish() {
        int[] cs = excludes.clone();
        sort(cs);
        int index = binarySearch(cs, -1);
        if (index < 0) {
            fill(excludes, -1);
            createRange(step);
        }
    }
}
