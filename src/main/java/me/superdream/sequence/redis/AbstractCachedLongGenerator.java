/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis;

/**
 * 带缓存的长整形自增序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public abstract class AbstractCachedLongGenerator extends AbstractGenerator<Long> {
    protected final int cachedNum; // 缓存的序列号数量

    protected long currSeqId; // 当前序列ID
    protected long maxSeqId; // 最大序列ID

    public AbstractCachedLongGenerator(String hname, String hkey, int cachedNum) {
        super(hname, hkey);
        this.cachedNum = cachedNum;
    }

    @Override
    public final Long next() {
        synchronized (this) {
            if (currSeqId == maxSeqId) {
                refresh();
            }

            currSeqId += 1L;
            return currSeqId;
        }
    }

    private void refresh() {
        refreshMaxSeqId();
        if (currSeqId == maxSeqId) {
            throw new IllegalStateException("无法刷新缓存的最大序列号");
        }
        currSeqId = maxSeqId - cachedNum;
    }

    /**
     * 刷新最大序列编号
     */
    protected abstract void refreshMaxSeqId();
}
