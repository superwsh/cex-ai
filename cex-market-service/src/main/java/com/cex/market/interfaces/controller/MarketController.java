package com.cex.market.interfaces.controller;

import com.cex.common.core.api.ApiResult;
import com.cex.market.application.service.MarketQueryService;
import com.cex.market.domain.model.MarketDepthSnapshot;
import com.cex.market.domain.trade.MarketTrade;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/** 面向客户端的只读市场 REST 接口。 */
@RestController
@RequestMapping("/api/v1/market")
@RequiredArgsConstructor
public class MarketController {

    private final MarketQueryService marketQueryService;
    private final Clock clock = Clock.systemUTC();

    /**
     * 查询已产生实际行情数据的交易对列表。
     *
     * @return 交易对列表
     */
    @GetMapping("/symbols")
    public ApiResult<List<String>> symbols() {
        return ApiResult.success(marketQueryService.symbols());
    }

    /**
     * 查询 24 小时行情；不传 symbol 时返回全部已就绪行情。
     *
     * @param symbol 交易对，可选
     * @return 单个或多个 24 小时行情
     */
    @GetMapping("/ticker/24hr")
    public ApiResult<Object> ticker24h(@RequestParam(required = false) String symbol) {
        return ApiResult.success(marketQueryService.ticker24h(symbol));
    }

    /**
     * 查询最佳买卖报价；不传 symbol 时返回全部已就绪报价。
     *
     * @param symbol 交易对，可选
     * @return 单个或多个最佳买卖报价
     */
    @GetMapping("/bookTicker")
    public ApiResult<Object> bookTicker(@RequestParam(required = false) String symbol) {
        return ApiResult.success(marketQueryService.bookTicker(symbol));
    }

    /**
     * 查询指定交易对的盘口深度。
     *
     * @param symbol 交易对
     * @param limit 每侧深度档位，仅支持 5、10、20、50、100、500，默认 20
     * @return 当前盘口快照
     */
    @GetMapping("/depth")
    public ApiResult<MarketDepthSnapshot> depth(@RequestParam String symbol,
                                                 @RequestParam(required = false) Integer limit) {
        return ApiResult.success(marketQueryService.depth(symbol, limit));
    }

    /**
     * 查询指定交易对的最近成交。
     *
     * @param symbol 交易对
     * @param limit 返回数量，默认 100、最大 1000
     * @return 最近成交列表
     */
    @GetMapping("/trades")
    public ApiResult<List<MarketTrade>> trades(@RequestParam String symbol,
                                               @RequestParam(required = false) Integer limit) {
        return ApiResult.success(marketQueryService.trades(symbol, limit));
    }

    /**
     * 查询服务当前时间。
     *
     * @return UTC 毫秒时间戳
     */
    @GetMapping("/time")
    public ApiResult<Map<String, Long>> time() {
        return ApiResult.success(Map.of("serverTime", clock.millis()));
    }
}
