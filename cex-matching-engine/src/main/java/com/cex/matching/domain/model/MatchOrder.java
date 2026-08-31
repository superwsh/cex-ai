package com.cex.matching.domain.model;

import com.cex.matching.domain.enums.OrderSide;
import com.cex.matching.domain.enums.OrderStatus;
import com.cex.matching.domain.enums.OrderType;
import com.cex.matching.domain.enums.TimeInForce;

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

    private MatchOrder(Builder builder) {
        validateBuilder(builder);
        this.orderId = builder.orderId;
        this.userId = builder.userId;
        this.symbol = builder.symbol;
        this.side = builder.side;
        this.type = builder.type;
        this.price = builder.price;
        this.quantity = builder.quantity;
        this.remainingQuantity = builder.quantity;
        this.quoteAmount = builder.quoteAmount;
        this.remainingQuoteAmount = builder.quoteAmount;
        this.timeInForce = builder.timeInForce;
        this.createdAt = builder.createdAt;
        this.sequence = builder.sequence;
        this.status = OrderStatus.OPEN;
    }

    /** 创建订单构造器，避免多参数构造函数。 */
    public static Builder builder() {
        return new Builder();
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

    private static void validateBuilder(Builder builder) {
        if (builder.orderId <= 0 || builder.userId <= 0 || builder.sequence < 0) {
            throw new IllegalArgumentException("订单标识和序号必须有效");
        }
        if (builder.symbol == null || builder.symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        Objects.requireNonNull(builder.side, "买卖方向不能为空");
        Objects.requireNonNull(builder.type, "订单类型不能为空");
        Objects.requireNonNull(builder.timeInForce, "有效期类型不能为空");
        Objects.requireNonNull(builder.createdAt, "创建时间不能为空");
        if (builder.type == OrderType.LIMIT) {
            requirePositive(builder.quantity, "订单数量");
            requirePositive(builder.price, "限价单价格");
            if (builder.quoteAmount != null) {
                throw new IllegalArgumentException("限价单计价资产预算必须为空");
            }
        } else if (builder.price != null) {
            throw new IllegalArgumentException("市价单价格必须为空");
        } else if (builder.side == OrderSide.BUY) {
            if (builder.quantity == null || builder.quantity.signum() != 0) {
                throw new IllegalArgumentException("市价买单数量必须为零");
            }
            requirePositive(builder.quoteAmount, "市价买单计价资产预算");
        } else {
            requirePositive(builder.quantity, "市价卖单数量");
            if (builder.quoteAmount != null) {
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

    /** 用于构造不可变订单属性的构造器。 */
    public static final class Builder {

        private long orderId;
        private long userId;
        private String symbol;
        private OrderSide side;
        private OrderType type;
        private BigDecimal price;
        private BigDecimal quantity;
        private BigDecimal quoteAmount;
        private TimeInForce timeInForce;
        private Instant createdAt;
        private long sequence;

        public Builder orderId(long orderId) {
            this.orderId = orderId;
            return this;
        }

        public Builder userId(long userId) {
            this.userId = userId;
            return this;
        }

        public Builder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }

        public Builder side(OrderSide side) {
            this.side = side;
            return this;
        }

        public Builder type(OrderType type) {
            this.type = type;
            return this;
        }

        public Builder price(BigDecimal price) {
            this.price = price;
            return this;
        }

        public Builder quantity(BigDecimal quantity) {
            this.quantity = quantity;
            return this;
        }

        /**
         * 设置市价买单可使用的计价资产预算。
         *
         * @param quoteAmount 计价资产预算
         * @return 当前构造器
         */
        public Builder quoteAmount(BigDecimal quoteAmount) {
            this.quoteAmount = quoteAmount;
            return this;
        }

        public Builder timeInForce(TimeInForce timeInForce) {
            this.timeInForce = timeInForce;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder sequence(long sequence) {
            this.sequence = sequence;
            return this;
        }

        /** 校验并创建订单。 */
        public MatchOrder build() {
            return new MatchOrder(this);
        }
    }
}
