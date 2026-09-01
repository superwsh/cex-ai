package com.cex.market.ws;

import com.cex.market.application.service.MarketDataPublisher;
import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 将已聚合的行情读模型分发给匹配频道的 WebSocket 客户端。 */
@Component
@RequiredArgsConstructor
public class MarketWebSocketPublisher implements MarketDataPublisher {

    private final MarketWebSocketBatchDispatcher batchDispatcher;

    /**
     * 发布逐笔成交频道。
     *
     * @param trade 已聚合成交
     */
    @Override
    public void publishTrade(MarketTrade trade) {
        MarketWebSocketChannel channel = new MarketWebSocketChannel(MarketWebSocketChannel.Type.TRADE, trade.symbol(), null);
        batchDispatcher.enqueueTrade("TRADE", channel.key(), trade);
    }

    /**
     * 发布 24 小时 Ticker 频道。
     *
     * @param ticker 24 小时 Ticker
     */
    @Override
    public void publishTicker(Ticker24h ticker) {
        conflate("TICKER", new MarketWebSocketChannel(MarketWebSocketChannel.Type.TICKER, ticker.symbol(), null), ticker);
    }

    /**
     * 发布最佳买卖报价频道。
     *
     * @param ticker 最佳买卖报价
     */
    @Override
    public void publishBookTicker(BookTicker ticker) {
        conflate("BOOK_TICKER", new MarketWebSocketChannel(MarketWebSocketChannel.Type.BOOK_TICKER, ticker.symbol(), null), ticker);
    }

    /**
     * 发布盘口深度频道。
     *
     * @param snapshot 盘口深度快照
     */
    @Override
    public void publishDepth(MarketDepthSnapshot snapshot) {
        conflate("DEPTH", new MarketWebSocketChannel(MarketWebSocketChannel.Type.DEPTH, snapshot.symbol(), null), snapshot);
    }

    /**
     * 发布 KLine 频道。
     *
     * @param kLine 当前 KLine
     */
    @Override
    public void publishKLine(KLine kLine) {
        conflate("KLINE", new MarketWebSocketChannel(MarketWebSocketChannel.Type.KLINE, kLine.symbol(), kLine.interval()), kLine);
    }

    /**
     * 将可合并行情交给短周期广播器，仅保留该频道最新快照。
     *
     * @param type 推送类型
     * @param channel 推送频道
     * @param data 行情数据
     */
    private void conflate(String type, MarketWebSocketChannel channel, Object data) {
        batchDispatcher.conflate(type, channel.key(), data);
    }
}
