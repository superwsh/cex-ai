package com.cex.market.application.service;

import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;

/** 当前未收线 KLine 的缓存端口。 */
public interface KLineCache {

    /**
     * 写入当前 KLine。
     *
     * @param kLine 当前未收线 KLine
     */
    void saveCurrent(KLine kLine);

    /**
     * 获取当前 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期
     * @return 当前 KLine；不存在时为空
     */
    KLine getCurrent(String symbol, KLineInterval interval);
}
