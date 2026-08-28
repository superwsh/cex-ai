package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("symbol_config")
public class SymbolConfigPO {

    @TableId(type = IdType.INPUT)
    private Long id;
    private String symbol;
    private String baseCurrency;
    private String quoteCurrency;
    private Integer priceScale;
    private Integer quantityScale;
    private BigDecimal minQuantity;
    private BigDecimal minAmount;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
