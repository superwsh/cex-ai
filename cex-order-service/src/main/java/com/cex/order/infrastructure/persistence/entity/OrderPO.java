package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
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
@TableName("orders")
public class OrderPO {

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private String side;
    private String type;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteAmount;
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private String status;
    private String timeInForce;
    @Version
    private Long version;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
