package com.cex.market.infrastructure.persistence;

import com.cex.market.application.service.KLineRepository;
import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;
import com.cex.market.infrastructure.persistence.entity.MarketKLinePO;
import com.cex.market.infrastructure.persistence.mapper.MarketKLineMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

/** MyBatis KLine 历史仓储实现。 */
@Repository
@RequiredArgsConstructor
public class MybatisKLineRepository implements KLineRepository {

    private final MarketKLineMapper marketKLineMapper;

    /**
     * 幂等持久化已收线 KLine。
     *
     * @param kLine 已收线 KLine
     */
    @Override
    public void upsertClosed(KLine kLine) {
        if (!kLine.closed()) {
            throw new IllegalArgumentException("禁止持久化未收线 KLine");
        }
        marketKLineMapper.upsertClosed(toPO(kLine));
    }

    /**
     * 查询已收线 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期
     * @param startTime 起始时间
     * @param endTime 结束时间
     * @param limit 最大返回数量
     * @return KLine 列表
     */
    @Override
    public List<KLine> findClosed(String symbol, KLineInterval interval, Long startTime, Long endTime, int limit) {
        return marketKLineMapper.findClosed(symbol, interval.getCode(), startTime, endTime, limit).stream()
                .map(this::toDomain).toList();
    }

    private MarketKLinePO toPO(KLine kLine) {
        MarketKLinePO po = new MarketKLinePO();
        po.setSymbol(kLine.symbol());
        po.setInterval(kLine.interval().getCode());
        po.setOpenTime(kLine.openTime());
        po.setCloseTime(kLine.closeTime());
        po.setOpenPrice(kLine.open());
        po.setHighPrice(kLine.high());
        po.setLowPrice(kLine.low());
        po.setClosePrice(kLine.close());
        po.setVolume(kLine.volume());
        po.setQuoteVolume(kLine.quoteVolume());
        po.setTradeCount(kLine.tradeCount());
        return po;
    }

    private KLine toDomain(MarketKLinePO po) {
        return new KLine(po.getSymbol(), KLineInterval.fromCode(po.getInterval()), po.getOpenTime(), po.getCloseTime(),
                po.getOpenPrice(), po.getHighPrice(), po.getLowPrice(), po.getClosePrice(), po.getVolume(),
                po.getQuoteVolume(), po.getTradeCount(), true);
    }
}
