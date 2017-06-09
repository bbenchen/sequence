/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence;

/**
 * 序列生成器接口
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public interface Generator<T extends Number> {

    /**
     * 获取下个序列编号
     *
     * @return 成功返回序列号，失败返回-1
     */
    T next();
}
