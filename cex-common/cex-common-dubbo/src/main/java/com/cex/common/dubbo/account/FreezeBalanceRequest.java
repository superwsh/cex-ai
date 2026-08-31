package com.cex.common.dubbo.account;

import java.io.Serializable;
import java.math.BigDecimal;

/** 账户冻结请求，bizType 与 bizId 构成业务幂等键。 */
public record FreezeBalanceRequest(Long userId, String asset, BigDecimal amount,
                                   String bizType, String bizId) implements Serializable {
}
