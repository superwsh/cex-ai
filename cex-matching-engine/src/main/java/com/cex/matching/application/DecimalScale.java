package com.cex.matching.application;

/** 交易对价格和数量的十进制位配置。 */
public record DecimalScale(int priceScale, int quantityScale) {

    public DecimalScale {
        if (priceScale < 0 || quantityScale < 0) {
            throw new IllegalArgumentException("小数位不能为负数");
        }
    }
}
