package com.cex.order.api.controller;

import com.cex.common.core.api.ApiResult;
import com.cex.order.api.request.CancelOrderRequest;
import com.cex.order.api.request.CreateOrderRequest;
import com.cex.order.api.request.OpenOrdersRequest;
import com.cex.order.api.request.OrderHistoryRequest;
import com.cex.order.api.response.CreateOrderResponse;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.api.response.PageResult;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.application.service.CancelOrderService;
import com.cex.order.application.service.CreateOrderService;
import com.cex.order.application.service.CreateOrderResult;
import com.cex.order.application.service.QueryOrderService;
import com.cex.order.domain.model.TimeInForce;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单接口:仅做参数接收/校验与结果返回,业务逻辑在 Application Service
 * 注意:当前无登录态,userId 从请求头 X-User-Id 读取(网关/后续接入认证后替换)
 */
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final CreateOrderService createOrderService;
    private final CancelOrderService cancelOrderService;
    private final QueryOrderService queryOrderService;

    /** 创建订单 */
    @PostMapping
    public ApiResult<CreateOrderResponse> createOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @Valid @RequestBody CreateOrderRequest request) {
        CreateOrderResult result = createOrderService.createOrder(toCommand(userId, request));
        return ApiResult.success(CreateOrderResponse.builder()
                .orderId(result.getOrderId())
                .status(result.getStatus().name())
                .build());
    }

    /** 取消订单 */
    @DeleteMapping("/{orderId}")
    public ApiResult<Void> cancelOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @PathVariable Long orderId) {
        cancelOrderService.cancelOrder(CancelOrderCommand.builder()
                .userId(userId).orderId(orderId).build());
        return ApiResult.success();
    }

    /** 查询订单 */
    @GetMapping("/{orderId}")
    public ApiResult<OrderResponse> getOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @PathVariable Long orderId) {
        return ApiResult.success(queryOrderService.getOrder(userId, orderId));
    }

    /** 查询当前委托 */
    @GetMapping("/open")
    public ApiResult<PageResult<OrderResponse>> openOrders(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            OpenOrdersRequest request) {
        return ApiResult.success(queryOrderService.listOpenOrders(
                userId, request.getSymbol(), request.getCursor(), request.getLimit()));
    }

    /** 查询历史订单 */
    @GetMapping
    public ApiResult<PageResult<OrderResponse>> historyOrders(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            OrderHistoryRequest request) {
        return ApiResult.success(queryOrderService.listHistoryOrders(
                userId, request.getSymbol(), request.getCursor(), request.getLimit()));
    }

    private CreateOrderCommand toCommand(Long userId, CreateOrderRequest request) {
        return CreateOrderCommand.builder()
                .userId(userId)
                .clientOrderId(request.getClientOrderId())
                .symbol(request.getSymbol())
                .side(request.getSide())
                .type(request.getType())
                .price(request.getPrice())
                .quantity(request.getQuantity())
                .quoteAmount(request.getQuoteAmount())
                .timeInForce(request.getTimeInForce() == null ? TimeInForce.GTC : request.getTimeInForce())
                .build();
    }
}
