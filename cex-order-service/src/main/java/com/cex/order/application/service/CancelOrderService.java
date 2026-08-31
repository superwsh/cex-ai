package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * 取消订单用例编排
 * 事务边界:状态置 CANCEL_REQUESTED + Outbox 事件在同一事务。
 * 说明:Phase 1 撮合引擎未接入,取消直接落库并发事件;撮合引擎接入后,
 *      最终状态以撮合回报为准(事件流按 symbol 保序,本流程结构无需变更)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CancelOrderService {

    private final OrderRepository orderRepository;
    private final SymbolConfigService symbolConfigService;
    private final OrderPersistenceService persistenceService;

    public void cancelOrder(CancelOrderCommand command) {
        // 归属 + 状态校验
        Order order = orderRepository.findByOrderId(command.getOrderId());
        if (order == null || !java.util.Objects.equals(command.getUserId(), order.getUserId())) {
            throw new BizException(ErrorCode.ORDER_NOT_FOUND.getCode(),
                    ErrorCode.ORDER_NOT_FOUND.getMessage() + ": " + command.getOrderId());
        }
        order.requestCancel(); // 仅申请撤单，必须等待撮合 ORDER_CANCELED 确认

        // 事务:置 CANCEL_REQUESTED + 写取消命令 Outbox
        SymbolConfig config = symbolConfigService.getRequired(order.getSymbol());
        persistenceService.cancelInTx(order, config);
    }
}
