package com.cex.clearing.application.service;

import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.common.kafka.event.TradeEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/** 成交事件入账前的业务完整性校验。 */
@Component
public class TradeEventValidator {

    /** 校验结算任务建档需要的全部成交事实。 */
    public void validate(TradeEvent event) {
        if (event == null || blank(event.getEventId()) || blank(event.getTradeId()) || blank(event.getSymbol())
                || blank(event.getBuyOrderId()) || blank(event.getSellOrderId()) || event.getBuyerUserId() == null
                || event.getSellerUserId() == null || event.getBuyerUserId() <= 0 || event.getSellerUserId() <= 0
                || blank(event.getBaseAsset()) || blank(event.getQuoteAsset()) || nonPositive(event.getPrice())
                || nonPositive(event.getQuantity()) || nonPositive(event.getAmount()) || event.getSequence() == null
                || event.getSequence() < 0 || event.getTimestamp() == null || event.getTimestamp() <= 0) {
            throw invalid("成交事件缺少结算所需字段");
        }
        if (event.getPrice().multiply(event.getQuantity()).compareTo(event.getAmount()) != 0) {
            throw invalid("成交金额与价格乘数量不一致");
        }
        if (event.getBuyerUserId().equals(event.getSellerUserId()) || event.getBaseAsset().equals(event.getQuoteAsset())) {
            throw invalid("成交买卖双方或资产对非法");
        }
        validateFee(event.getBuyerFee(), event.getBuyerFeeAsset(), "买方手续费");
        validateFee(event.getSellerFee(), event.getSellerFeeAsset(), "卖方手续费");
    }

    private void validateFee(BigDecimal fee, String asset, String fieldName) {
        if (fee == null || fee.signum() < 0 || (fee.signum() > 0 && blank(asset))) {
            throw invalid(fieldName + "字段非法");
        }
    }

    private SettlementException invalid(String message) {
        return new SettlementException("INVALID_TRADE", message, false);
    }

    private boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private boolean nonPositive(BigDecimal value) {
        return value == null || value.signum() <= 0;
    }
}
