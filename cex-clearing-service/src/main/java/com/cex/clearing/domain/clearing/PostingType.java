package com.cex.clearing.domain.clearing;

/** 清算计算生成的账户过账类型。 */
public enum PostingType {
    BUYER_QUOTE_FROZEN_DEBIT,
    BUYER_BASE_AVAILABLE_CREDIT,
    SELLER_BASE_FROZEN_DEBIT,
    SELLER_QUOTE_AVAILABLE_CREDIT,
    PLATFORM_FEE_CREDIT
}
