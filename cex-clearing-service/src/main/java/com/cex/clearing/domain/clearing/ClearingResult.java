package com.cex.clearing.domain.clearing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** 一笔成交的不可变清算结果。 */
public record ClearingResult(String tradeId, List<AccountPosting> postings) {

    public ClearingResult {
        if (tradeId == null || tradeId.isBlank()) {
            throw new IllegalArgumentException("成交ID不能为空");
        }
        postings = List.copyOf(Objects.requireNonNull(postings, "过账集合不能为空"));
        if (postings.isEmpty()) {
            throw new IllegalArgumentException("清算结果必须包含过账");
        }
        assertAssetConservation(postings);
    }

    /** 验证每种资产的可用与冻结合计变化为零，防止资产凭空产生或消失。 */
    private static void assertAssetConservation(List<AccountPosting> postings) {
        Map<String, BigDecimal> changes = postings.stream().collect(Collectors.groupingBy(AccountPosting::asset,
                Collectors.reducing(BigDecimal.ZERO,
                        posting -> posting.availableChange().add(posting.frozenChange()), BigDecimal::add)));
        boolean unbalanced = changes.values().stream().anyMatch(change -> change.signum() != 0);
        if (unbalanced) {
            throw new IllegalArgumentException("清算结果未满足资产守恒");
        }
    }
}
