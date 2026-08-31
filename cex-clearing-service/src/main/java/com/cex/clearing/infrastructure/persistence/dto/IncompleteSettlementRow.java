package com.cex.clearing.infrastructure.persistence.dto;

import lombok.Data;

/** Trade vs Settlement 扫描得到的未成功结算任务。 */
@Data
public class IncompleteSettlementRow {
    private String tradeId;
    private String settlementStatus;
}
