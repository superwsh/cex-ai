package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/** 撮合引擎使用的内存订单模型。 */
public final class MatchOrder {

    private final long orderId;
    private final long userId;
    private final String symbol;
    private final OrderSide side;
    private final OrderType type;
    private final BigDecimal price;
    private final BigDecimal quantity;
    private BigDecimal remainingQuantity;
    private final BigDecimal quoteAmount;
    private BigDecimal remainingQuoteAmount;
    private BigDecimal filledQuantity = BigDecimal.ZERO;
    private final TimeInForce timeInForce;
    private final Instant createdAt;
    private final long sequence;
    private OrderStatus status;

    /**
     * 创建订单并初始化可变成交状态；由 Lombok 生成的 Builder 调用。
     *
     * @param orderId 订单编号
     * @param userId 用户编号
     * @param symbol 交易对
     * @param side 买卖方向
     * @param type 订单类型
     * @param price 限价单价格
     * @param quantity 基础资产数量
     * @param quoteAmount 市价买单计价资产预算
     * @param timeInForce 有效期类型
     * @param createdAt 创建时间
     * @param sequence 撮合序号
     */
    @Builder
    private MatchOrder(long orderId, long userId, String symbol, OrderSide side, OrderType type,
                       BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount,
                       TimeInForce timeInForce, Instant createdAt, long sequence) {
        validate(orderId, userId, symbol, side, type, price, quantity, quoteAmount, timeInForce, createdAt, sequence);
        this.orderId = orderId;
        this.userId = userId;
        this.symbol = symbol;
        this.side = side;
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        this.remainingQuantity = quantity;
        this.quoteAmount = quoteAmount;
        this.remainingQuoteAmount = quoteAmount;
        this.timeInForce = timeInForce;
        this.createdAt = createdAt;
        this.sequence = sequence;
        this.status = OrderStatus.OPEN;
    }

    /**
     * 按数量记录普通订单的成交并更新订单状态。
     *
     * @param filledQuantity 本次成交数量
     */
    public void decreaseRemainingQuantity(BigDecimal filledQuantity) {
        if (isQuoteBudgetMarketBuy()) {
            throw new IllegalStateException("市价买单必须同时按成交数量和成交金额扣减");
        }
        applyFill(filledQuantity, null);
    }

    /**
     * 同时按成交数量和成交金额记录成交，并按订单类型推进状态机。
     *
     * @param filledQuantity 本次成交数量
     * @param filledQuoteAmount 本次成交金额；仅市价买单必须提供
     */
    public void applyFill(BigDecimal filledQuantity, BigDecimal filledQuoteAmount) {
        requirePositive(filledQuantity, "成交数量");
        if (!status.canFill()) {
            throw new IllegalStateException("当前订单状态不允许成交: " + status);
        }
        if (isQuoteBudgetMarketBuy()) {
            decreaseMarketBuyBudget(filledQuantity, filledQuoteAmount);
            return;
        }
        BigDecimal newRemainingQuantity = remainingQuantity.subtract(filledQuantity);
        if (newRemainingQuantity.signum() < 0) {
            throw new IllegalArgumentException("成交数量超过订单剩余数量");
        }
        remainingQuantity = newRemainingQuantity;
        this.filledQuantity = this.filledQuantity.add(filledQuantity);
        status = newRemainingQuantity.signum() == 0
                ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }

    /**
     * 扣减市价买单的计价资产预算，并记录实际获得的基础资产数量。
     *
     * @param filledQuantity 本次获得的基础资产数量
     * @param filledQuoteAmount 本次消耗的计价资产金额
     */
    private void decreaseMarketBuyBudget(BigDecimal filledQuantity, BigDecimal filledQuoteAmount) {
        requirePositive(filledQuoteAmount, "成交金额");
        BigDecimal newRemainingQuoteAmount = remainingQuoteAmount.subtract(filledQuoteAmount);
        if (newRemainingQuoteAmount.signum() < 0) {
            throw new IllegalArgumentException("成交金额超过市价买单剩余预算");
        }
        remainingQuoteAmount = newRemainingQuoteAmount;
        this.filledQuantity = this.filledQuantity.add(filledQuantity);
        status = newRemainingQuoteAmount.signum() == 0
                ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
    }

    /** 按状态机撤销订单保留的剩余数量。 */
    public void cancel() {
        if (!status.canCancel()) {
            throw new IllegalStateException("当前订单状态不允许撤销: " + status);
        }
        status = OrderStatus.CANCELED;
    }

    public long getOrderId() {
        return orderId;
    }

    public long getUserId() {
        return userId;
    }

    public String getSymbol() {
        return symbol;
    }

    public OrderSide getSide() {
        return side;
    }

    public OrderType getType() {
        return type;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public BigDecimal getQuantity() {
        return quantity;
    }

    public BigDecimal getRemainingQuantity() {
        return remainingQuantity;
    }

    /**
     * 获取市价买单尚未消耗的计价资产预算；其他订单返回 null。
     *
     * @return 剩余计价资产预算
     */
    public BigDecimal getRemainingQuoteAmount() {
        return remainingQuoteAmount;
    }

    /**
     * 获取市价买单的初始计价资产预算；其他订单返回 null。
     *
     * @return 初始计价资产预算
     */
    public BigDecimal getQuoteAmount() {
        return quoteAmount;
    }

    /**
     * 获取订单已成交的基础资产数量。
     *
     * @return 已成交数量
     */
    public BigDecimal getFilledQuantity() {
        return filledQuantity;
    }

    /**
     * 判断订单是否还有可继续撮合的数量或市价买单预算。
     *
     * @return 仍可继续撮合时为 true
     */
    public boolean hasRemainingToMatch() {
        return isQuoteBudgetMarketBuy()
                ? remainingQuoteAmount.signum() > 0 : remainingQuantity.signum() > 0;
    }

    /**
     * 判断订单是否已经发生过至少一笔成交。
     *
     * @return 已成交数量大于零时为 true
     */
    public boolean hasAnyFill() {
        return filledQuantity.signum() > 0;
    }

    public TimeInForce getTimeInForce() {
        return timeInForce;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public long getSequence() {
        return sequence;
    }

    public OrderStatus getStatus() {
        return status;
    }

    private static void validate(long orderId, long userId, String symbol, OrderSide side, OrderType type,
                                 BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount,
                                 TimeInForce timeInForce, Instant createdAt, long sequence) {
        if (orderId <= 0 || userId <= 0 || sequence < 0) {
            throw new IllegalArgumentException("订单标识和序号必须有效");
        }
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Objects.requireNonNull(side, "买卖方向不能为空");
        Objects.requireNonNull(type, "订单类型不能为空");
        Objects.requireNonNull(timeInForce, "有效期类型不能为空");
        Objects.requireNonNull(createdAt, "创建时间不能为空");
        if (type == OrderType.LIMIT) {
            requirePositive(quantity, "订单数量");
            requirePositive(price, "限价单价格");
            if (quoteAmount != null) {
                throw new IllegalArgumentException("限价单计价资产预算必须为空");
            }
        } else if (price != null) {
            throw new IllegalArgumentException("市价单价格必须为空");
        } else if (side == OrderSide.BUY) {
            if (quantity == null || quantity.signum() != 0) {
                throw new IllegalArgumentException("市价买单数量必须为零");
            }
            requirePositive(quoteAmount, "市价买单计价资产预算");
        } else {
            requirePositive(quantity, "市价卖单数量");
            if (quoteAmount != null) {
                throw new IllegalArgumentException("市价卖单计价资产预算必须为空");
            }
        }
    }

    /**
     * 判断当前订单是否为使用计价资产预算成交的市价买单。
     *
     * @return 市价买单时为 true
     */
    private boolean isQuoteBudgetMarketBuy() {
        return type == OrderType.MARKET && side == OrderSide.BUY;
    }

    private static BigDecimal requirePositive(BigDecimal value, String fieldName) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(fieldName + "必须大于零");
        }
        return value;
    }

}
