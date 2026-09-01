package com.cex.market.interfaces.controller;

import com.cex.common.core.api.ApiResult;
import com.cex.market.application.service.KLineQueryService;
import com.cex.market.domain.kline.KLine;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/** 行情 KLine 查询接口；其余行情 REST 接口在 Phase 5 提供。 */
@RestController
@RequestMapping("/api/v1/market")
@RequiredArgsConstructor
public class MarketKLineController {

    private final KLineQueryService kLineQueryService;

    /**
     * 查询指定交易对和周期的历史及当前 KLine。
     *
     * @param symbol 交易对
     * @param interval 周期：1m、5m、15m、1h、4h、1d
     * @param startTime 起始开盘时间（毫秒），可选
     * @param endTime 结束开盘时间（毫秒），可选
     * @param limit 最大返回数量，默认 500、最大 1000
     * @return KLine 列表
     */
    @GetMapping("/klines")
    public ApiResult<List<KLine>> klines(@RequestParam String symbol, @RequestParam String interval,
                                         @RequestParam(required = false) Long startTime,
                                         @RequestParam(required = false) Long endTime,
                                         @RequestParam(required = false) Integer limit) {
        return ApiResult.success(kLineQueryService.query(symbol, interval, startTime, endTime, limit));
    }
}
