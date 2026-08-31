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
    private BigDecimal cancelConfirmedFilledQuantity;
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
        if (shouldConfirmCanceled(newFilledQty)) {
            this.status = OrderStatus.CANCELED;
            this.updatedAt = LocalDateTime.now();
            return;
        }
        this.status = marketBuy
                ? (quoteAmount != null && newFilledAmount.compareTo(quoteAmount) >= 0
                        ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED)
                : (newFilledQty.compareTo(quantity) >= 0
                        ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED);
        this.updatedAt = LocalDateTime.now();
    }

    public void cancel() {
        confirmCanceled(safeFilledQuantity());
    }

    /** 记录用户撤单申请，资金仍保持冻结直到撮合引擎确认。 */
    public void requestCancel() {
        if (!status.canCancel()) {
            throw new OrderStatusInvalidException("订单状态 " + status + " 不允许取消");
        }
        this.status = OrderStatus.CANCEL_REQUESTED;
        this.updatedAt = LocalDateTime.now();
    }

    /** 记录撮合撤单确认，并在本地已接收全部成交回报后进入 CANCELED。 */
    public boolean confirmCanceled(BigDecimal confirmedFilledQuantity) {
        if (confirmedFilledQuantity == null || confirmedFilledQuantity.signum() < 0) {
            throw new OrderStatusInvalidException("撤单确认累计成交数量非法");
        }
        if (status == OrderStatus.CANCELED) {
            return false;
        }
        if (status == OrderStatus.FILLED || status == OrderStatus.REJECTED) {
            throw new OrderStatusInvalidException("终态订单不可确认撤单: " + status);
        }
        this.cancelConfirmedFilledQuantity = confirmedFilledQuantity;
        if (safeFilledQuantity().compareTo(confirmedFilledQuantity) > 0) {
            throw new OrderStatusInvalidException("本地累计成交数量超过撮合撤单确认数量");
        }
        if (safeFilledQuantity().compareTo(confirmedFilledQuantity) == 0) {
            this.status = OrderStatus.CANCELED;
            this.updatedAt = LocalDateTime.now();
            return true;
        }
        return false;
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

    private boolean shouldConfirmCanceled(BigDecimal filledQuantity) {
        return status == OrderStatus.CANCEL_REQUESTED && cancelConfirmedFilledQuantity != null
                && filledQuantity.compareTo(cancelConfirmedFilledQuantity) == 0;
    }

    public boolean isOpen() {
        return status.canCancel();
    }
}
