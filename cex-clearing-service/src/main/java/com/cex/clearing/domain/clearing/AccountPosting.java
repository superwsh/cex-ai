package com.cex.clearing.domain.clearing;

import java.math.BigDecimal;

/** 单个账户余额变化，不包含变更前后快照。 */
public record AccountPosting(Long userId, String asset, BigDecimal availableChange,
                             BigDecimal frozenChange, PostingType type) {

    public AccountPosting {
        if (userId == null || asset == null || asset.isBlank() || availableChange == null || frozenChange == null
                || type == null || (availableChange.signum() == 0 && frozenChange.signum() == 0)) {
            throw new IllegalArgumentException("账户过账字段非法");
        }
    }
}
