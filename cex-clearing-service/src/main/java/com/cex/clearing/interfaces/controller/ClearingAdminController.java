package com.cex.clearing.interfaces.controller;

import com.cex.clearing.application.service.ClearingAdminOperationService;
import com.cex.clearing.application.service.ClearingAdminQueryService;
import com.cex.clearing.config.ClearingAdminAuthorizationInterceptor;
import com.cex.clearing.interfaces.dto.AdminPageResponse;
import com.cex.clearing.interfaces.dto.BalanceFlowView;
import com.cex.clearing.interfaces.dto.ManualActionRequest;
import com.cex.clearing.interfaces.dto.ReconciliationView;
import com.cex.clearing.interfaces.dto.SettlementView;
import com.cex.common.core.api.ApiResult;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 清算后台查询和受控人工操作接口；全部路径由后台鉴权拦截器保护。 */
@RestController
@RequestMapping("/api/admin/clearing")
@RequiredArgsConstructor
public class ClearingAdminController {

    private final ClearingAdminQueryService queryService;
    private final ClearingAdminOperationService operationService;

    /** 按成交 ID 查询结算任务。 */
    @GetMapping("/settlements/{tradeId}")
    public ApiResult<SettlementView> getSettlement(@PathVariable String tradeId) {
        return ApiResult.success(queryService.getSettlement(tradeId));
    }

    /** 分页查询结算任务。 */
    @GetMapping("/settlements")
    public ApiResult<AdminPageResponse<SettlementView>> listSettlements(
            @RequestParam(required = false) Long userId, @RequestParam(required = false) String status,
            @RequestParam(required = false) Integer pageNo, @RequestParam(required = false) Integer pageSize) {
        return ApiResult.success(queryService.listSettlements(userId, status, pageNo, pageSize));
    }

    /** 分页查询资金流水。 */
    @GetMapping("/ledger/flows")
    public ApiResult<AdminPageResponse<BalanceFlowView>> listFlows(
            @RequestParam(required = false) Long userId, @RequestParam(required = false) String asset,
            @RequestParam(required = false) String bizId, @RequestParam(required = false) Integer pageNo,
            @RequestParam(required = false) Integer pageSize) {
        return ApiResult.success(queryService.listFlows(userId, asset, bizId, pageNo, pageSize));
    }

    /** 分页查询对账问题。 */
    @GetMapping("/reconciliation/issues")
    public ApiResult<AdminPageResponse<ReconciliationView>> listReconciliationIssues(
            @RequestParam(required = false) String type, @RequestParam(required = false) String status,
            @RequestParam(required = false) Integer pageNo, @RequestParam(required = false) Integer pageSize) {
        return ApiResult.success(queryService.listReconciliationIssues(type, status, pageNo, pageSize));
    }

    /** 将人工复核结算重新排入恢复队列。 */
    @PostMapping("/settlements/{tradeId}/retry")
    public ApiResult<Void> retrySettlement(
            @PathVariable String tradeId,
            @RequestHeader(ClearingAdminAuthorizationInterceptor.OPERATOR_HEADER) String operatorId,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            @Valid @RequestBody ManualActionRequest request) {
        operationService.retrySettlement(tradeId, operatorId, requestId, request.getReason());
        return ApiResult.success();
    }

    /** 重新核验单个对账问题。 */
    @PostMapping("/reconciliation/{issueId}/recheck")
    public ApiResult<Boolean> recheckReconciliation(
            @PathVariable Long issueId,
            @RequestHeader(ClearingAdminAuthorizationInterceptor.OPERATOR_HEADER) String operatorId,
            @RequestHeader(value = "X-Request-Id", required = false) String requestId,
            @Valid @RequestBody ManualActionRequest request) {
        return ApiResult.success(operationService.recheckReconciliation(
                issueId, operatorId, requestId, request.getReason()));
    }
}
