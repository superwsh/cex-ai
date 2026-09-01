package com.cex.market.ws;

import com.cex.market.domain.kline.KLineInterval;

/** WebSocket 行情频道的已校验标识。 */
public record MarketWebSocketChannel(Type type, String symbol, KLineInterval interval) {

    /** 支持推送的行情频道类型。 */
    public enum Type {
        TRADE("trade"),
        TICKER("ticker"),
        BOOK_TICKER("bookTicker"),
        DEPTH("depth"),
        KLINE("kline");

        private final String code;

        Type(String code) {
            this.code = code;
        }

        /**
         * 获取协议中的频道类型编码。
         *
         * @return 频道编码
         */
        public String getCode() {
            return code;
        }

        /**
         * 从协议编码解析频道类型。
         *
         * @param code 客户端传入编码
         * @return 对应频道类型
         */
        public static Type fromCode(String code) {
            for (Type type : values()) {
                if (type.code.equalsIgnoreCase(code)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("不支持的行情频道类型: " + code);
        }
    }

    /**
     * 校验已解析的频道字段。
     *
     * @param type 频道类型
     * @param symbol 交易对
     * @param interval KLine 周期；非 KLine 频道必须为空
     */
    public MarketWebSocketChannel {
        if (type == null || symbol == null || symbol.isBlank() || symbol.contains(".") || symbol.chars().anyMatch(Character::isWhitespace)
                || type == Type.KLINE && interval == null || type != Type.KLINE && interval != null) {
            throw new IllegalArgumentException("行情频道字段非法");
        }
    }

    /**
     * 从客户端订阅文本解析频道。
     *
     * @param value 频道文本，如 trade.BTC_USDT 或 kline.BTC_USDT.1m
     * @return 已校验频道
     */
    public static MarketWebSocketChannel parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("频道不能为空");
        }
        String[] parts = value.split("\\.", -1);
        Type type = Type.fromCode(parts[0]);
        if (type == Type.KLINE) {
            if (parts.length != 3) {
                throw new IllegalArgumentException("KLine 频道格式应为 kline.{symbol}.{interval}");
            }
            return new MarketWebSocketChannel(type, parts[1], KLineInterval.fromCode(parts[2]));
        }
        if (parts.length != 2) {
            throw new IllegalArgumentException("频道格式应为 " + type.getCode() + ".{symbol}");
        }
        return new MarketWebSocketChannel(type, parts[1], null);
    }

    /**
     * 返回用于订阅索引的规范频道名称。
     *
     * @return 规范频道名称
     */
    public String key() {
        return interval == null ? type.getCode() + "." + symbol
                : type.getCode() + "." + symbol + "." + interval.getCode();
    }
}
