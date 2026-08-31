package com.cex.clearing.domain.clearing;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.common.kafka.event.TradeEvent;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 现货成交清算计算器，只生成过账计划，不访问任何外部系统。 */
public final class ClearingCalculator {

    public static final long PLATFORM_USER_ID = 0L;

    private final FeeCalculator feeCalculator;

    /**
     * 创建清算计算器。
     *
     * @param feeCalculator 提供成交时已固定的手续费
     */
    public ClearingCalculator(FeeCalculator feeCalculator) {
        this.feeCalculator = Objects.requireNonNull(feeCalculator, "手续费计算器不能为空");
    }

    /**
     * 计算成交的买卖双方与平台费用账户过账计划。
     *
     * @param tradeEvent 已通过消费者字段校验的成交事件
     * @return 满足各资产守恒关系的不可变清算结果
     */
    public ClearingResult calculate(TradeEvent tradeEvent) {
        validateTrade(tradeEvent);
        Fee buyerFee = feeCalculator.calculateBuyerFee(tradeEvent);
        Fee sellerFee = feeCalculator.calculateSellerFee(tradeEvent);
        validateFeeAssets(tradeEvent, buyerFee, sellerFee);

        BigDecimal buyerBaseCredit = tradeEvent.getQuantity().subtract(buyerFee.amount());
        BigDecimal sellerQuoteCredit = tradeEvent.getAmount().subtract(sellerFee.amount());
        if (buyerBaseCredit.signum() <= 0 || sellerQuoteCredit.signum() <= 0) {
            throw invalidFee("手续费不能覆盖全部成交资产");
        }

        List<AccountPosting> postings = new ArrayList<>();
        postings.add(posting(tradeEvent.getBuyerUserId(), tradeEvent.getQuoteAsset(), BigDecimal.ZERO,
                tradeEvent.getAmount().negate(), PostingType.BUYER_QUOTE_FROZEN_DEBIT));
        postings.add(posting(tradeEvent.getBuyerUserId(), tradeEvent.getBaseAsset(), buyerBaseCredit,
                BigDecimal.ZERO, PostingType.BUYER_BASE_AVAILABLE_CREDIT));
        postings.add(posting(tradeEvent.getSellerUserId(), tradeEvent.getBaseAsset(), BigDecimal.ZERO,
                tradeEvent.getQuantity().negate(), PostingType.SELLER_BASE_FROZEN_DEBIT));
        postings.add(posting(tradeEvent.getSellerUserId(), tradeEvent.getQuoteAsset(), sellerQuoteCredit,
                BigDecimal.ZERO, PostingType.SELLER_QUOTE_AVAILABLE_CREDIT));
        addPlatformFeePosting(postings, buyerFee);
        addPlatformFeePosting(postings, sellerFee);
        return new ClearingResult(tradeEvent.getTradeId(), postings);
    }

    private void addPlatformFeePosting(List<AccountPosting> postings, Fee fee) {
        if (fee.amount().signum() > 0) {
            postings.add(posting(PLATFORM_USER_ID, fee.asset(), fee.amount(), BigDecimal.ZERO,
                    PostingType.PLATFORM_FEE_CREDIT));
        }
    }

    private AccountPosting posting(Long userId, String asset, BigDecimal availableChange,
                                   BigDecimal frozenChange, PostingType type) {
        return new AccountPosting(userId, asset, availableChange, frozenChange, type);
    }

    private void validateTrade(TradeEvent event) {
        if (event == null || event.getBuyerUserId() == null || event.getSellerUserId() == null
                || event.getBuyerUserId() <= 0 || event.getSellerUserId() <= 0 || blank(event.getTradeId())
                || blank(event.getBaseAsset()) || blank(event.getQuoteAsset()) || nonPositive(event.getQuantity())
                || nonPositive(event.getPrice()) || nonPositive(event.getAmount())) {
            throw new SettlementException("INVALID_TRADE", "成交清算输入非法", false);
        }
        if (event.getBuyerUserId().equals(event.getSellerUserId()) || event.getBaseAsset().equals(event.getQuoteAsset())) {
            throw new SettlementException("INVALID_TRADE", "成交买卖双方或资产对非法", false);
        }
        if (event.getPrice().multiply(event.getQuantity()).compareTo(event.getAmount()) != 0) {
            throw new SettlementException("INVALID_TRADE", "成交金额与价格、数量不一致", false);
        }
    }

    private void validateFeeAssets(TradeEvent event, Fee buyerFee, Fee sellerFee) {
        if (buyerFee.amount().signum() > 0 && !event.getBaseAsset().equals(buyerFee.asset())) {
            throw invalidFee("买方手续费资产必须为基础资产");
        }
        if (sellerFee.amount().signum() > 0 && !event.getQuoteAsset().equals(sellerFee.asset())) {
            throw invalidFee("卖方手续费资产必须为计价资产");
        }
        if (buyerFee.amount().compareTo(event.getQuantity()) >= 0 || sellerFee.amount().compareTo(event.getAmount()) >= 0) {
            throw invalidFee("手续费超过可收取资产");
        }
    }

    private SettlementException invalidFee(String message) {
        return new SettlementException("INVALID_FEE", message, false);
    }

    private boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private boolean nonPositive(BigDecimal value) {
        return value == null || value.signum() <= 0;
    }
}
