package com.cex.order.application.service;

import com.baomidou.mybatisplus.core.exceptions.MybatisPlusException;
import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.domain.service.TradingRuleValidator;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.FreezeRequest;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * 创建订单用例编排
 * 事务边界:
 *   - 冻结(RPC)在事务外:失败不落订单
 *   - 订单 + Outbox 在同一本地事务(OrderPersistenceService)
 *   - 本地事务失败 -> 补偿解冻,冻结不会永久存在
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CreateOrderService {

    private final OrderRepository orderRepository;
    private final SymbolConfigService symbolConfigService;
    private final TradingRuleValidator ruleValidator;
    private final FreezeCalculator freezeCalculator;
    private final AccountServiceClient accountServiceClient;
    private final OrderPersistenceService persistenceService;
    private final SnowflakeGenerator snowflakeGenerator;

    public CreateOrderResult createOrder(CreateOrderCommand command) {
        // 1. 幂等预查:重复 clientOrderId 直接返回首次结果,不重复冻结
        Order existing = orderRepository.findByUserIdAndClientOrderId(
                command.getUserId(), command.getClientOrderId());
        if (existing != null) {
            log.info("幂等命中,返回首次结果: userId={}, clientOrderId={}, orderId={}",
                    command.getUserId(), command.getClientOrderId(), existing.getOrderId());
            return CreateOrderResult.of(existing);
        }

        // 2-5. 交易对与规则校验(不存在/暂停/精度/最小数量/最小金额)
        SymbolConfig config = symbolConfigService.getRequired(command.getSymbol());
        ruleValidator.validatePrice(command.getPrice(), config);
        // 市价买单按 quoteAmount 冻结,无数量概念,跳过数量校验(裁定 2)
        if (!(command.getType() == OrderType.MARKET && command.getSide() == OrderSide.BUY)) {
            ruleValidator.validateQuantity(command.getQuantity(), config);
        }
        ruleValidator.validateMinAmount(command.getSide(), command.getType(),
                command.getQuoteAmount() != null ? command.getQuoteAmount() : command.getPrice(),
                command.getQuantity(), config);

        // 6. 订单 ID(先于冻结生成,bizId=orderId 保证冻结幂等)
        Long orderId = snowflakeGenerator.nextId();

        // 7-8. 计算冻结金额并冻结(事务外)
        BigDecimal freezeAmount = freezeCalculator.calculate(command.getSide(), command.getType(),
                command.getPrice(), command.getQuantity(), command.getQuoteAmount());
        String currency = freezeCalculator.freezeCurrency(command.getSide(), config);
        try {
            accountServiceClient.freeze(FreezeRequest.builder()
                    .userId(command.getUserId())
                    .currency(currency)
                    .amount(freezeAmount)
                    .bizType("FREEZE_ORDER")
                    .bizId(orderId)
                    .build());
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            log.error("资产冻结异常: userId={}, orderId={}, currency={}", command.getUserId(), orderId, currency, e);
            throw new BizException(ErrorCode.FREEZE_FAILED.getCode(), ErrorCode.FREEZE_FAILED.getMessage());
        }

        // 9. 本地事务:订单 + Outbox
        try {
            return persistenceService.createOrderInTx(command, orderId, config);
        } catch (Exception e) {
            // 10. 补偿:本地事务失败必须解冻,防止冻结永久存在
            log.error("订单落库失败,补偿解冻: orderId={}, userId={}", orderId, command.getUserId(), e);
            try {
                accountServiceClient.unfreeze(UnfreezeRequest.builder()
                        .userId(command.getUserId())
                        .currency(currency)
                        .amount(freezeAmount)
                        .bizType("FREEZE_ORDER")
                        .bizId(orderId)
                        .build());
            } catch (Exception ex) {
                log.error("补偿解冻失败,需人工介入: orderId={}, userId={}, amount={}",
                        orderId, command.getUserId(), freezeAmount, ex);
            }
            throw e;
        }
    }
}
