package com.cex.clearing.application.service;

import com.cex.clearing.infrastructure.persistence.entity.AdminOperationAuditPO;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.AdminOperationAuditMapper;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationResultMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 清算后台人工重试和对账复核审计测试。 */
@ExtendWith(MockitoExtension.class)
class ClearingAdminOperationServiceTest {

    @Mock private SettlementTaskMapper settlementTaskMapper;
    @Mock private ReconciliationResultMapper reconciliationResultMapper;
    @Mock private AdminOperationAuditMapper auditMapper;
    @Mock private ReconciliationApplicationService reconciliationApplicationService;

    private ClearingAdminOperationService operationService;

    @BeforeEach
    void setUp() {
        operationService = new ClearingAdminOperationService(settlementTaskMapper, reconciliationResultMapper,
                auditMapper, reconciliationApplicationService);
    }

    /** 人工重试必须排入 RETRY 并写入操作人、原因和前后状态。 */
    @Test
    void shouldScheduleManualRetryAndWriteAuditInSameOperation() {
        SettlementTaskPO task = new SettlementTaskPO();
        task.setTradeId("T-1");
        task.setStatus("MANUAL_REVIEW");
        when(settlementTaskMapper.selectOne(any())).thenReturn(task);
        when(settlementTaskMapper.scheduleManualRetry(any(), any())).thenReturn(1);
        when(auditMapper.insert(any(AdminOperationAuditPO.class))).thenReturn(1);

        operationService.retrySettlement("T-1", "operator-1", "request-1", "账户已修复");

        verify(settlementTaskMapper).scheduleManualRetry(any(), any());
        ArgumentCaptor<AdminOperationAuditPO> captor = ArgumentCaptor.forClass(AdminOperationAuditPO.class);
        verify(auditMapper).insert(captor.capture());
        assertThat(captor.getValue().getOperatorId()).isEqualTo("operator-1");
        assertThat(captor.getValue().getBeforeStatus()).isEqualTo("MANUAL_REVIEW");
        assertThat(captor.getValue().getAfterStatus()).isEqualTo("RETRY");
    }

    /** 对账差异消失时应标记 CONFIRMED 并保留复核审计。 */
    @Test
    void shouldConfirmResolvedReconciliationAndWriteAudit() {
        ReconciliationResultPO issue = new ReconciliationResultPO();
        issue.setId(1L);
        issue.setStatus("OPEN");
        when(reconciliationResultMapper.selectById(1L)).thenReturn(issue);
        when(reconciliationApplicationService.isResolved(issue)).thenReturn(true);
        when(reconciliationResultMapper.updateById(issue)).thenReturn(1);
        when(auditMapper.insert(any(AdminOperationAuditPO.class))).thenReturn(1);

        boolean resolved = operationService.recheckReconciliation(1L, "operator-1", "request-2", "重新核验");

        assertThat(resolved).isTrue();
        assertThat(issue.getStatus()).isEqualTo("CONFIRMED");
        assertThat(issue.getResolvedAt()).isNotNull();
        verify(auditMapper).insert(any(AdminOperationAuditPO.class));
    }
}
