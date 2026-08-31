package com.cex.clearing.domain.clearing;

import java.math.BigDecimal;

/** 已固定的单方成交手续费。 */
public record Fee(BigDecimal amount, String asset) {

    public Fee {
        if (amount == null || amount.signum() < 0) {
            throw new IllegalArgumentException("手续费金额不能为负数");
        }
        if (amount.signum() > 0 && (asset == null || asset.isBlank())) {
            throw new IllegalArgumentException("非零手续费必须指定资产");
        }
    }
}
