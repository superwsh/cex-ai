package com.cex.market.domain.model;

import java.math.BigDecimal;

/** 行情读取模型中的不可变聚合价格档位。 */
public record MarketPriceLevel(BigDecimal price, BigDecimal quantity) {

    /**
     * 创建有效的非零聚合价格档位。
     *
     * @param price 档位价格
     * @param quantity 档位聚合数量
     */
    public MarketPriceLevel {
        if (price == null || price.signum() <= 0 || quantity == null || quantity.signum() <= 0) {
            throw new IllegalArgumentException("行情价格档位非法");
        }
    }
}
