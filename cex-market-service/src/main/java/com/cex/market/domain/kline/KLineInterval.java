package com.cex.market.domain.kline;

import java.util.Arrays;

/** 第一阶段支持的 UTC KLine 周期。 */
public enum KLineInterval {
    ONE_MINUTE("1m", 60_000L),
    FIVE_MINUTES("5m", 5 * 60_000L),
    FIFTEEN_MINUTES("15m", 15 * 60_000L),
    ONE_HOUR("1h", 60 * 60_000L),
    FOUR_HOURS("4h", 4 * 60 * 60_000L),
    ONE_DAY("1d", 24 * 60 * 60_000L);

    private final String code;
    private final long durationMillis;

    KLineInterval(String code, long durationMillis) {
        this.code = code;
        this.durationMillis = durationMillis;
    }

    /**
     * 按协议编码解析周期。
     *
     * @param code 周期编码
     * @return 支持的周期
     */
    public static KLineInterval fromCode(String code) {
        return Arrays.stream(values()).filter(interval -> interval.code.equals(code)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("不支持的 KLine 周期: " + code));
    }

    /**
     * 将 UTC 时间戳对齐到周期开始。
     *
     * @param timestamp 时间戳（毫秒）
     * @return 周期开始时间
     */
    public long openTimeOf(long timestamp) {
        if (timestamp <= 0) {
            throw new IllegalArgumentException("KLine 时间戳必须大于零");
        }
        return timestamp / durationMillis * durationMillis;
    }

    public String getCode() {
        return code;
    }

    public long getDurationMillis() {
        return durationMillis;
    }
}
