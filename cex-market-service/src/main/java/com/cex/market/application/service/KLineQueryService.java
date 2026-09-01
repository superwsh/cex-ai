package com.cex.market.application.service;

import com.cex.market.domain.kline.KLine;
import com.cex.market.domain.kline.KLineInterval;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** 查询历史与当前 KLine 的应用服务。 */
@Service
@RequiredArgsConstructor
public class KLineQueryService {

    public static final int DEFAULT_LIMIT = 500;
    public static final int MAX_LIMIT = 1_000;

    private final KLineRepository kLineRepository;
    private final KLineCache kLineCache;

    /**
     * 查询指定交易对、周期和时间范围的 KLine。
     *
     * @param symbol 交易对
     * @param intervalCode 周期编码
     * @param startTime 起始时间，可为空
     * @param endTime 结束时间，可为空
     * @param limit 最大数量，可为空
     * @return 按开盘时间升序排列的 KLine
     */
    public List<KLine> query(String symbol, String intervalCode, Long startTime, Long endTime, Integer limit) {
        validateRequest(symbol, startTime, endTime);
        KLineInterval interval = KLineInterval.fromCode(intervalCode);
        int actualLimit = limit == null ? DEFAULT_LIMIT : limit;
        if (actualLimit <= 0 || actualLimit > MAX_LIMIT) {
            throw new IllegalArgumentException("KLine limit 必须在 1 到 " + MAX_LIMIT + " 之间");
        }
        List<KLine> klines = new ArrayList<>(kLineRepository.findClosed(symbol, interval, startTime, endTime, actualLimit));
        KLine current = kLineCache.getCurrent(symbol, interval);
        if (current != null && inRange(current, startTime, endTime)) {
            klines.removeIf(item -> item.openTime() == current.openTime());
            klines.add(current);
        }
        klines.sort(Comparator.comparingLong(KLine::openTime));
        return klines.size() <= actualLimit ? List.copyOf(klines)
                : List.copyOf(klines.subList(klines.size() - actualLimit, klines.size()));
    }

    private void validateRequest(String symbol, Long startTime, Long endTime) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        if (startTime != null && startTime <= 0 || endTime != null && endTime <= 0
                || startTime != null && endTime != null && startTime > endTime) {
            throw new IllegalArgumentException("KLine 时间范围非法");
        }
    }

    private boolean inRange(KLine kLine, Long startTime, Long endTime) {
        return (startTime == null || kLine.openTime() >= startTime)
                && (endTime == null || kLine.openTime() <= endTime);
    }
}
