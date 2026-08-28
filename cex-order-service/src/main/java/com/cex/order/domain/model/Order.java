package com.cex.order.domain.model;

import com.cex.order.common.OrderStatusInvalidException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单聚合根:状态流转必须通过领域方法,禁止直接 setStatus
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private Long id;                 // 主键 = orderId
    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    private BigDecimal price;        // 市价单为 null
    private BigDecimal quantity;
    private BigDecimal quoteAmount;  // 市价买单冻结金额
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private OrderStatus status;
    private TimeInForce timeInForce;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Long version;

    /** 提交撮合(预留:创建后状态即为 PENDING_MATCH) */
    public void markPendingMatch() {
        transition(OrderStatus.NEW, OrderStatus.PENDING_MATCH);
    }

    /**
     * 成交回报:累计已成交数量/金额,按状态机流转
     * 市价买单 quantity 恒为 ZERO,完成判断走金额维度(quoteAmount vs filledAmount)
     */
    public void markPartiallyFilled(BigDecimal fillQuantity, BigDecimal fillAmount) {
        if (!status.canFill()) {
            throw new OrderStatusInvalidException("订单状态 " + status + " 不允许成交");
        }
        boolean marketBuy = type == OrderType.MARKET && side == OrderSide.BUY;
        BigDecimal newFilledQty = safeFilledQuantity().add(fillQuantity);
        if (!marketBuy && newFilledQty.compareTo(quantity) > 0) {
            throw new OrderStatusInvalidException("成交数量超过委托数量: " + newFilledQty + " > " + quantity);
        }
        BigDecimal newFilledAmount = safeFilledAmount().add(fillAmount);
        if (marketBuy && quoteAmount != null && newFilledAmount.compareTo(quoteAmount) > 0) {
            throw new OrderStatusInvalidException("成交金额超过冻结金额: " + newFilledAmount + " > " + quoteAmount);
        }
        this.filledQuantity = newFilledQty;
        this.filledAmount = newFilledAmount;
        this.status = marketBuy
                ? (quoteAmount != null && newFilledAmount.compareTo(quoteAmount) >= 0
                        ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED)
                : (newFilledQty.compareTo(quantity) >= 0
                        ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED);
        this.updatedAt = LocalDateTime.now();
    }

    public void cancel() {
        if (!status.canCancel()) {
            throw new OrderStatusInvalidException("订单状态 " + status + " 不允许取消");
        }
        this.status = OrderStatus.CANCELED;
        this.updatedAt = LocalDateTime.now();
    }

    public void reject() {
        if (status.isTerminal()) {
            throw new OrderStatusInvalidException("终态订单不可拒绝: " + status);
        }
        this.status = OrderStatus.REJECTED;
        this.updatedAt = LocalDateTime.now();
    }

    private void transition(OrderStatus from, OrderStatus to) {
        if (this.status != from) {
            throw new OrderStatusInvalidException("订单状态 " + this.status + " 不允许流转到 " + to);
        }
        this.status = to;
        this.updatedAt = LocalDateTime.now();
    }

    private BigDecimal safeFilledQuantity() {
        return filledQuantity == null ? BigDecimal.ZERO : filledQuantity;
    }

    private BigDecimal safeFilledAmount() {
        return filledAmount == null ? BigDecimal.ZERO : filledAmount;
    }

    public boolean isOpen() {
        return status.canCancel();
    }
}
