/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.ssdb;

import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 带缓存的长整形自增序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class CachedLongGenerator extends AbstractGenerator<Long> {
    private static final Logger log = LoggerFactory.getLogger(CachedLongGenerator.class);

    private final int cachedNum; // 缓存的序列号数量

    private long minSeqId; // 最小序列编号
    private long currSeqId; // 当前序列编号

    public CachedLongGenerator(SSDB ssdb, String hname, String hkey, int cachedNum) {
        super(ssdb, hname, hkey);
        this.cachedNum = cachedNum;
        refresh();
    }

    @Override
    public Long next() {
        synchronized (this) {
            try {
                if (minSeqId + cachedNum == currSeqId) {
                    refresh();
                }
                currSeqId = minSeqId + 1L;
                return currSeqId;
            } catch (Exception ex) {
                if (log.isErrorEnabled()) {
                    log.error("Cannot get the increment serial number from SSDB");
                }
                return -1L;
            }
        }
    }

    private void refresh() {
        Response res = ssdb.hincr(hname, hkey, cachedNum);
        minSeqId = res.asLong() - cachedNum;
    }
}
