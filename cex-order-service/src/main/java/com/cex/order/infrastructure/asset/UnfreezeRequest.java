package com.cex.order.infrastructure.asset;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UnfreezeRequest {
    private Long userId;
    private String currency;
    private BigDecimal amount;
    private String bizType;
    private Long bizId;
}
