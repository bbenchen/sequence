/*
 * Copyright (c) 2017 SuperDream Inc. <http://www.superdream.me>
 */

package me.superdream.sequence.ssdb;

import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 整形自增序列生成器
 *
 * @author <a href="mailto:517926804@qq.com">Mike Chen</a>
 * @version 0.1.0
 * @since 0.1.0
 */
public class IntegerGenerator extends AbstractGenerator<Integer> {
    private static final Logger log = LoggerFactory.getLogger(IntegerGenerator.class);

    public IntegerGenerator(SSDB ssdb, String hname, String hkey) {
        super(ssdb, hname, hkey);
    }

    @Override
    public Integer next() {
        try {
            Response res = ssdb.hincr(hname, hkey, 1);
            return res.ok() ? res.asInt() : -1;
        } catch (Exception ex) {
            if (log.isErrorEnabled()) {
                log.error("Cannot get the increment serial number from SSDB", ex);
            }
            return -1;
        }
    }
}
