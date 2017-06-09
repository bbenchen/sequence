/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.redis;

import me.superdream.sequence.Generator;

/**
 * 基于Redis的抽象序列生成器
 * <p>
 *     使用Redis的HashMap数据结构来保存序列生成器相关信息
 * </p>
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public abstract class AbstractGenerator<T extends Number> implements Generator<T> {
    protected final String hname; // HashMap数据结构的名称
    protected final String hkey; // 序列号关联HashMap的Key

    public AbstractGenerator(String hname, String hkey) {
        this.hname = hname;
        this.hkey = hkey;
    }
}
