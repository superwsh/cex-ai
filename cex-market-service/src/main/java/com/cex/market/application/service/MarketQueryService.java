package com.cex.market.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.BookTicker;
import com.cex.market.domain.trade.MarketTrade;
import com.cex.market.domain.trade.Ticker24h;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/** 面向 REST 接口读取 Redis 热行情的应用服务。 */
@Service
@RequiredArgsConstructor
public class MarketQueryService {

    public static final int DEFAULT_TRADE_LIMIT = 100;
    public static final int MAX_TRADE_LIMIT = 1_000;
    private static final List<Integer> DEPTH_LIMITS = List.of(5, 10, 20, 50, 100, 500);

    private final MarketDataCache marketDataCache;

    /**
     * 查询已产生实际行情数据的交易对。
     *
     * @return 已登记交易对列表
     */
    public List<String> symbols() {
        return marketDataCache.findSymbols();
    }

    /**
     * 查询单个或全部交易对的 24 小时行情。
     *
     * @param symbol 交易对；为空时查询全部
     * @return 单个交易对或全部交易对的行情数据
     */
    public Object ticker24h(String symbol) {
        if (symbol != null) {
            return requireTicker(symbol);
        }
        return symbols().stream().map(marketDataCache::findTicker24h).filter(ticker -> ticker != null).toList();
    }

    /**
     * 查询单个或全部交易对的最佳买卖报价。
     *
     * @param symbol 交易对；为空时查询全部
     * @return 单个交易对或全部交易对的最佳买卖报价
     */
    public Object bookTicker(String symbol) {
        if (symbol != null) {
            return requireBookTicker(symbol);
        }
        return symbols().stream().map(marketDataCache::findBookTicker).filter(ticker -> ticker != null).toList();
    }

    /**
     * 查询一个交易对的盘口深度。
     *
     * @param symbol 交易对
     * @param limit 档位数
     * @return 当前盘口深度快照
     */
    public MarketDepthSnapshot depth(String symbol, Integer limit) {
        String requiredSymbol = requireSymbol(symbol);
        int actualLimit = limit == null ? 20 : limit;
        if (!DEPTH_LIMITS.contains(actualLimit)) {
            throw new BizException(400, "depth limit 仅支持 " + DEPTH_LIMITS);
        }
        MarketDepthSnapshot snapshot = marketDataCache.findDepthSnapshot(requiredSymbol, actualLimit);
        if (snapshot == null) {
            throw marketDataNotReady(requiredSymbol, "深度");
        }
        return snapshot;
    }

    /**
     * 查询一个交易对的最近成交。
     *
     * @param symbol 交易对
     * @param limit 最大返回数量；为空时使用默认值
     * @return 按成交时间倒序的最近成交列表
     */
    public List<MarketTrade> trades(String symbol, Integer limit) {
        String requiredSymbol = requireSymbol(symbol);
        int actualLimit = limit == null ? DEFAULT_TRADE_LIMIT : limit;
        if (actualLimit <= 0 || actualLimit > MAX_TRADE_LIMIT) {
            throw new BizException(400, "trades limit 必须在 1 到 " + MAX_TRADE_LIMIT + " 之间");
        }
        return marketDataCache.findRecentTrades(requiredSymbol, actualLimit);
    }

    /**
     * 校验并获取单交易对的 24 小时行情。
     *
     * @param symbol 交易对
     * @return 24 小时行情
     */
    private Ticker24h requireTicker(String symbol) {
        String requiredSymbol = requireSymbol(symbol);
        Ticker24h ticker = marketDataCache.findTicker24h(requiredSymbol);
        if (ticker == null) {
            throw marketDataNotReady(requiredSymbol, "24 小时行情");
        }
        return ticker;
    }

    /**
     * 校验并获取单交易对的最佳买卖报价。
     *
     * @param symbol 交易对
     * @return 最佳买卖报价
     */
    private BookTicker requireBookTicker(String symbol) {
        String requiredSymbol = requireSymbol(symbol);
        BookTicker ticker = marketDataCache.findBookTicker(requiredSymbol);
        if (ticker == null) {
            throw marketDataNotReady(requiredSymbol, "最佳买卖报价");
        }
        return ticker;
    }

    /**
     * 校验交易对参数。
     *
     * @param symbol 交易对
     * @return 已校验的交易对
     */
    private String requireSymbol(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new BizException(400, "symbol 不能为空");
        }
        return symbol;
    }

    /**
     * 创建市场数据尚未就绪的统一业务异常。
     *
     * @param symbol 交易对
     * @param dataType 数据类型
     * @return 业务异常
     */
    private BizException marketDataNotReady(String symbol, String dataType) {
        return new BizException(404, "交易对 " + symbol + " 的" + dataType + "尚未就绪");
    }
}
