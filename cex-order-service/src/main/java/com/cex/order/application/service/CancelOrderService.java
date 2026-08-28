package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * 取消订单用例编排
 * 事务边界:状态置 CANCELED + Outbox 事件在同一事务;解冻在事务外
 * 说明:Phase 1 撮合引擎未接入,取消直接落库并发事件;撮合引擎接入后,
 *      最终状态以撮合回报为准(事件流按 symbol 保序,本流程结构无需变更)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CancelOrderService {

    private final OrderRepository orderRepository;
    private final SymbolConfigService symbolConfigService;
    private final AccountServiceClient accountServiceClient;
    private final OrderPersistenceService persistenceService;
    private final FreezeCalculator freezeCalculator;

    public void cancelOrder(CancelOrderCommand command) {
        // 归属 + 状态校验
        Order order = orderRepository.findByOrderId(command.getOrderId());
        if (order == null || !java.util.Objects.equals(command.getUserId(), order.getUserId())) {
            throw new BizException(ErrorCode.ORDER_NOT_FOUND.getCode(),
                    ErrorCode.ORDER_NOT_FOUND.getMessage() + ": " + command.getOrderId());
        }
        order.cancel(); // 非法状态抛 OrderStatusInvalidException(如 FILLED)

        // 事务:置 CANCELED + 写取消事件 Outbox
        persistenceService.cancelInTx(order);

        // 事务外:解冻剩余冻结资产
        SymbolConfig config = symbolConfigService.getRequired(order.getSymbol());
        BigDecimal unfreezeAmount = freezeCalculator.remainingToUnfreeze(order, config);
        if (unfreezeAmount.compareTo(BigDecimal.ZERO) > 0) {
            try {
                accountServiceClient.unfreeze(UnfreezeRequest.builder()
                        .userId(order.getUserId())
                        .currency(freezeCalculator.freezeCurrency(order.getSide(), config))
                        .amount(unfreezeAmount)
                        .bizType("FREEZE_ORDER")
                        .bizId(order.getOrderId())
                        .build());
            } catch (Exception e) {
                log.error("取消订单解冻失败,需人工介入: orderId={}, userId={}, amount={}",
                        order.getOrderId(), order.getUserId(), unfreezeAmount, e);
            }
        }
    }
}
