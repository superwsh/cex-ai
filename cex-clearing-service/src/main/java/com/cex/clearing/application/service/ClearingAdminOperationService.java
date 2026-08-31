package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.clearing.common.ClearingErrorCode;
import com.cex.clearing.infrastructure.persistence.entity.AdminOperationAuditPO;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.AdminOperationAuditMapper;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationResultMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.common.core.exception.BizException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/** 执行受控后台操作，并保证状态变更与操作审计在同一事务提交。 */
@Service
@RequiredArgsConstructor
public class ClearingAdminOperationService {

    private final SettlementTaskMapper settlementTaskMapper;
    private final ReconciliationResultMapper reconciliationResultMapper;
    private final AdminOperationAuditMapper auditMapper;
    private final ReconciliationApplicationService reconciliationApplicationService;

    /** 将人工复核或失败结算重新排入恢复队列。 */
    @Transactional
    public void retrySettlement(String tradeId, String operatorId, String requestId, String reason) {
        validateAuditContext(operatorId, reason);
        SettlementTaskPO task = settlementTaskMapper.selectOne(new LambdaQueryWrapper<SettlementTaskPO>()
                .eq(SettlementTaskPO::getTradeId, tradeId));
        if (task == null) {
            throw exception(ClearingErrorCode.SETTLEMENT_NOT_FOUND);
        }
        if (settlementTaskMapper.scheduleManualRetry(tradeId, LocalDateTime.now()) != 1) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
        writeAudit("SETTLEMENT_RETRY", tradeId, operatorId, requestId, reason, task.getStatus(), "RETRY");
    }

    /** 重查单个对账问题；差异消失时标记 CONFIRMED，否则继续保持 OPEN。 */
    @Transactional
    public boolean recheckReconciliation(Long issueId, String operatorId, String requestId, String reason) {
        validateAuditContext(operatorId, reason);
        ReconciliationResultPO issue = reconciliationResultMapper.selectById(issueId);
        if (issue == null) {
            throw exception(ClearingErrorCode.RECONCILIATION_NOT_FOUND);
        }
        boolean resolved = reconciliationApplicationService.isResolved(issue);
        String beforeStatus = issue.getStatus();
        issue.setStatus(resolved ? "CONFIRMED" : "OPEN");
        issue.setResolvedAt(resolved ? LocalDateTime.now() : null);
        issue.setUpdatedAt(LocalDateTime.now());
        if (reconciliationResultMapper.updateById(issue) != 1) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
        writeAudit("RECONCILIATION_RECHECK", String.valueOf(issueId), operatorId, requestId, reason,
                beforeStatus, issue.getStatus());
        return resolved;
    }

    /** 写入不可省略的操作人、原因和请求标识。 */
    private void writeAudit(String operationType, String targetId, String operatorId, String requestId,
                            String reason, String beforeStatus, String afterStatus) {
        AdminOperationAuditPO audit = new AdminOperationAuditPO();
        audit.setOperationType(operationType);
        audit.setTargetId(targetId);
        audit.setOperatorId(operatorId);
        audit.setRequestId(requestId);
        audit.setReason(reason);
        audit.setBeforeStatus(beforeStatus);
        audit.setAfterStatus(afterStatus);
        audit.setCreatedAt(LocalDateTime.now());
        if (auditMapper.insert(audit) != 1) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
    }

    /** 校验人工操作审计上下文。 */
    private void validateAuditContext(String operatorId, String reason) {
        if (operatorId == null || operatorId.isBlank() || reason == null || reason.isBlank() || reason.length() > 256) {
            throw exception(ClearingErrorCode.INVALID_ADMIN_OPERATION);
        }
    }

    private BizException exception(ClearingErrorCode errorCode) {
        return new BizException(errorCode.getCode(), errorCode.getMessage());
    }
}
