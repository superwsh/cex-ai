package com.cex.market.application.service;

import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;

import java.util.List;

/** 已收线 KLine 的持久化端口。 */
public interface KLineRepository {

    /**
     * 幂等写入一根已收线 KLine。
     *
     * @param kLine 已收线 KLine
     */
    void upsertClosed(KLine kLine);

    /**
     * 查询指定时间范围内的已收线 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期
     * @param startTime 起始时间，可为空
     * @param endTime 结束时间，可为空
     * @param limit 最大返回数量
     * @return 按开盘时间升序排列的 KLine
     */
    List<KLine> findClosed(String symbol, KLineInterval interval, Long startTime, Long endTime, int limit);
}
