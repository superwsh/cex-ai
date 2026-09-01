package com.cex.market.domain.model;

import java.util.List;

/**
 * 一个交易对在确定盘口序号下的不可变 Level 2 深度快照。
 * 后续 REST、Redis 缓存和恢复流程均以该领域模型作为读取边界。
 */
public record MarketDepthSnapshot(String symbol, long sequence, List<MarketPriceLevel> bids,
                                  List<MarketPriceLevel> asks, long timestamp) {

    /**
     * 创建深度快照并防御性复制所有价格档位。
     *
     * @param symbol 交易对
     * @param sequence 快照包含的最后盘口序号
     * @param bids 价格从高到低的买方档位
     * @param asks 价格从低到高的卖方档位
     * @param timestamp 快照创建时间（毫秒时间戳）
     */
    public MarketDepthSnapshot {
        if (symbol == null || symbol.isBlank() || sequence < 0 || timestamp <= 0) {
            throw new IllegalArgumentException("盘口快照字段非法");
        }
        bids = copyLevels(bids, "买方档位");
        asks = copyLevels(asks, "卖方档位");
    }

    /**
     * 深复制并校验快照档位，避免外部修改影响已生成的快照。
     *
     * @param levels 原始价格档位
     * @param fieldName 字段名称
     * @return 不可修改的档位副本
     */
    private static List<MarketPriceLevel> copyLevels(List<MarketPriceLevel> levels, String fieldName) {
        if (levels == null) {
            throw new IllegalArgumentException(fieldName + "不能为空");
        }
        return levels.stream().map(level -> {
            if (level == null || level.price() == null || level.price().signum() <= 0
                    || level.quantity() == null || level.quantity().signum() <= 0) {
                throw new IllegalArgumentException(fieldName + "包含非法价格档位");
            }
            return new MarketPriceLevel(level.price(), level.quantity());
        }).toList();
    }
}
