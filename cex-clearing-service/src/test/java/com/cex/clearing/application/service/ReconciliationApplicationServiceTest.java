package com.cex.clearing.application.service;

import com.cex.clearing.infrastructure.persistence.dto.AccountLedgerSnapshotRow;
import com.cex.clearing.infrastructure.persistence.dto.IncompleteSettlementRow;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationReadMapper;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationResultMapper;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 9 三类对账差异扫描测试。 */
@ExtendWith(MockitoExtension.class)
class ReconciliationApplicationServiceTest {

    @Mock private ReconciliationReadMapper reconciliationReadMapper;
    @Mock private ReconciliationResultMapper reconciliationResultMapper;
    @Mock private ClearingMetrics clearingMetrics;
    @Mock private ClearingAlertService clearingAlertService;

    private ReconciliationApplicationService reconciliationApplicationService;

    @BeforeEach
    void setUp() {
        reconciliationApplicationService = new ReconciliationApplicationService(reconciliationReadMapper,
                reconciliationResultMapper, clearingMetrics, clearingAlertService);
    }

    /** 应将未成功结算、缺账务资料和账户余额差异分别记录为独立问题。 */
    @Test
    void shouldPersistAllThreeKindsOfReconciliationIssues() {
        when(reconciliationReadMapper.selectIncompleteSettlements(any(), anyInt()))
                .thenReturn(List.of(incompleteSettlement("T-1", "RETRY")));
        when(reconciliationReadMapper.selectSettlementLedgerInconsistencies(anyInt()))
                .thenReturn(List.of(incompleteSettlement("T-2", "SUCCESS")));
        when(reconciliationReadMapper.selectAccountLedgerInconsistencies(anyInt()))
                .thenReturn(List.of(accountDifference()));

        ReconciliationApplicationService.ReconciliationSummary summary = reconciliationApplicationService.reconcile();

        assertThat(summary.tradeSettlementIssues()).isEqualTo(1);
        assertThat(summary.settlementLedgerIssues()).isEqualTo(1);
        assertThat(summary.accountLedgerIssues()).isEqualTo(2);
        ArgumentCaptor<ReconciliationResultPO> captor = ArgumentCaptor.forClass(ReconciliationResultPO.class);
        verify(reconciliationResultMapper, times(4)).upsertOpen(captor.capture());
        assertThat(captor.getAllValues()).anySatisfy(issue -> {
            assertThat(issue.getReconciliationType()).isEqualTo("TRADE_SETTLEMENT");
            assertThat(issue.getBizId()).isEqualTo("T-1");
            assertThat(issue.getExpectedAmount()).isEqualByComparingTo("1");
            assertThat(issue.getActualAmount()).isZero();
        });
        assertThat(captor.getAllValues()).anySatisfy(issue -> {
            assertThat(issue.getReconciliationType()).isEqualTo("ACCOUNT_LEDGER");
            assertThat(issue.getCheckItem()).isEqualTo("AVAILABLE");
            assertThat(issue.getDifference()).isEqualByComparingTo("2");
        });
        assertThat(captor.getAllValues()).anySatisfy(issue -> {
            assertThat(issue.getReconciliationType()).isEqualTo("ACCOUNT_LEDGER");
            assertThat(issue.getCheckItem()).isEqualTo("FROZEN");
            assertThat(issue.getDifference()).isEqualByComparingTo("-1");
        });
    }

    /** 无任何差异时不得写入对账结果。 */
    @Test
    void shouldNotPersistResultWhenNoDifferenceExists() {
        when(reconciliationReadMapper.selectIncompleteSettlements(any(), anyInt())).thenReturn(List.of());
        when(reconciliationReadMapper.selectSettlementLedgerInconsistencies(anyInt())).thenReturn(List.of());
        when(reconciliationReadMapper.selectAccountLedgerInconsistencies(anyInt())).thenReturn(List.of());

        ReconciliationApplicationService.ReconciliationSummary summary = reconciliationApplicationService.reconcile();

        assertThat(summary.totalIssues()).isZero();
        verify(reconciliationResultMapper, never()).upsertOpen(any());
    }

    /** 构造未成功或账务不完整的成交记录。 */
    private IncompleteSettlementRow incompleteSettlement(String tradeId, String status) {
        IncompleteSettlementRow row = new IncompleteSettlementRow();
        row.setTradeId(tradeId);
        row.setSettlementStatus(status);
        return row;
    }

    /** 构造可用与冻结余额均不一致的账户快照。 */
    private AccountLedgerSnapshotRow accountDifference() {
        AccountLedgerSnapshotRow row = new AccountLedgerSnapshotRow();
        row.setUserId(100L);
        row.setAsset("USDT");
        row.setExpectedAvailable(new BigDecimal("100"));
        row.setActualAvailable(new BigDecimal("102"));
        row.setExpectedFrozen(new BigDecimal("5"));
        row.setActualFrozen(new BigDecimal("4"));
        return row;
    }
}
