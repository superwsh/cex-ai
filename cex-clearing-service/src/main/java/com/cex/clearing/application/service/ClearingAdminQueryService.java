package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.cex.clearing.common.ClearingErrorCode;
import com.cex.clearing.domain.reconciliation.ReconciliationType;
import com.cex.clearing.domain.settlement.SettlementStatus;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.BalanceFlowMapper;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationResultMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.clearing.interfaces.dto.AdminPageResponse;
import com.cex.clearing.interfaces.dto.BalanceFlowView;
import com.cex.clearing.interfaces.dto.ReconciliationView;
import com.cex.clearing.interfaces.dto.SettlementView;
import com.cex.common.core.exception.BizException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Set;

/** 清算后台只读查询服务。 */
@Service
@RequiredArgsConstructor
public class ClearingAdminQueryService {

    private static final Set<String> RECONCILIATION_STATUSES = Set.of("OPEN", "CONFIRMED", "COMPENSATED", "IGNORED");
    private final SettlementTaskMapper settlementTaskMapper;
    private final BalanceFlowMapper balanceFlowMapper;
    private final ReconciliationResultMapper reconciliationResultMapper;

    /** 按成交 ID 查询结算任务。 */
    public SettlementView getSettlement(String tradeId) {
        SettlementTaskPO task = settlementTaskMapper.selectOne(new LambdaQueryWrapper<SettlementTaskPO>()
                .eq(SettlementTaskPO::getTradeId, tradeId));
        if (task == null) {
            throw exception(ClearingErrorCode.SETTLEMENT_NOT_FOUND);
        }
        return SettlementView.from(task);
    }

    /** 按用户和状态分页查询结算任务。 */
    public AdminPageResponse<SettlementView> listSettlements(Long userId, String status, Integer pageNo, Integer pageSize) {
        validateSettlementStatus(status);
        LambdaQueryWrapper<SettlementTaskPO> query = new LambdaQueryWrapper<SettlementTaskPO>();
        if (userId != null) {
            query.and(wrapper -> wrapper.eq(SettlementTaskPO::getBuyerUserId, userId)
                    .or().eq(SettlementTaskPO::getSellerUserId, userId));
        }
        query.eq(status != null && !status.isBlank(), SettlementTaskPO::getStatus, status)
                .orderByDesc(SettlementTaskPO::getCreatedAt).orderByDesc(SettlementTaskPO::getId);
        Page<SettlementTaskPO> page = settlementTaskMapper.selectPage(page(pageNo, pageSize), query);
        return response(page, page.getRecords().stream().map(SettlementView::from).toList());
    }

    /** 分页查询不可变资金流水。 */
    public AdminPageResponse<BalanceFlowView> listFlows(Long userId, String asset, String bizId,
                                                        Integer pageNo, Integer pageSize) {
        LambdaQueryWrapper<BalanceFlowPO> query = new LambdaQueryWrapper<BalanceFlowPO>()
                .eq(userId != null, BalanceFlowPO::getUserId, userId)
                .eq(asset != null && !asset.isBlank(), BalanceFlowPO::getAsset, asset)
                .eq(bizId != null && !bizId.isBlank(), BalanceFlowPO::getBizId, bizId)
                .orderByDesc(BalanceFlowPO::getCreatedAt).orderByDesc(BalanceFlowPO::getId);
        Page<BalanceFlowPO> page = balanceFlowMapper.selectPage(page(pageNo, pageSize), query);
        return response(page, page.getRecords().stream().map(BalanceFlowView::from).toList());
    }

    /** 分页查询对账问题。 */
    public AdminPageResponse<ReconciliationView> listReconciliationIssues(String type, String status,
                                                                           Integer pageNo, Integer pageSize) {
        validateReconciliationFilter(type, status);
        LambdaQueryWrapper<ReconciliationResultPO> query = new LambdaQueryWrapper<ReconciliationResultPO>()
                .eq(type != null && !type.isBlank(), ReconciliationResultPO::getReconciliationType, type)
                .eq(status != null && !status.isBlank(), ReconciliationResultPO::getStatus, status)
                .orderByDesc(ReconciliationResultPO::getCreatedAt).orderByDesc(ReconciliationResultPO::getId);
        Page<ReconciliationResultPO> page = reconciliationResultMapper.selectPage(page(pageNo, pageSize), query);
        return response(page, page.getRecords().stream().map(ReconciliationView::from).toList());
    }

    /** 统一限制分页大小，防止后台查询拖垮资金库。 */
    private <T> Page<T> page(Integer pageNo, Integer pageSize) {
        int safePageNo = pageNo == null || pageNo < 1 ? 1 : pageNo;
        int safePageSize = pageSize == null || pageSize < 1 || pageSize > 100 ? 20 : pageSize;
        return new Page<>(safePageNo, safePageSize);
    }

    /** 将 MyBatis 分页结果转换为稳定 API 结构。 */
    private <T, V> AdminPageResponse<V> response(Page<T> page, java.util.List<V> items) {
        return new AdminPageResponse<>(items, page.getTotal(), page.getCurrent(), page.getSize());
    }

    /** 校验结算状态过滤条件。 */
    private void validateSettlementStatus(String status) {
        if (status == null || status.isBlank()) {
            return;
        }
        try {
            SettlementStatus.valueOf(status);
        } catch (IllegalArgumentException exception) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
    }

    /** 校验对账类型和状态过滤条件。 */
    private void validateReconciliationFilter(String type, String status) {
        try {
            if (type != null && !type.isBlank()) {
                ReconciliationType.valueOf(type);
            }
        } catch (IllegalArgumentException exception) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
        if (status != null && !status.isBlank() && !RECONCILIATION_STATUSES.contains(status)) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
    }

    private BizException exception(ClearingErrorCode errorCode) {
        return new BizException(errorCode.getCode(), errorCode.getMessage());
    }
}
