/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.ssdb;

import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 带缓存的整形自增序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class CachedIntegerGenerator extends AbstractGenerator<Integer> {
    private static final Logger log = LoggerFactory.getLogger(CachedIntegerGenerator.class);

    private final int cachedNum; // 缓存的序列号数量

    private int minSeqId; // 最小序列编号
    private int currSeqId; // 当前序列编号

    public CachedIntegerGenerator(SSDB ssdb, String hname, String hkey, int cachedNum) {
        super(ssdb, hname, hkey);
        this.cachedNum = cachedNum;
        refresh();
    }

    @Override
    public Integer next() {
        synchronized (this) {
            try {
                if (minSeqId + cachedNum == currSeqId) {
                    refresh();
                }
                currSeqId = minSeqId + 1;
                return currSeqId;
            } catch (Exception ex) {
                if (log.isErrorEnabled()) {
                    log.error("Cannot get the increment serial number from SSDB", ex);
                }
                return -1;
            }
        }
    }

    private void refresh() {
        Response res = ssdb.hincr(hname, hkey, cachedNum);
        minSeqId = res.asInt() - cachedNum;
    }
}
