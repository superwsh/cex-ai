package com.cex.clearing.application.service;

import com.cex.clearing.domain.reconciliation.ReconciliationType;
import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.clearing.infrastructure.persistence.dto.AccountLedgerSnapshotRow;
import com.cex.clearing.infrastructure.persistence.dto.IncompleteSettlementRow;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationReadMapper;
import com.cex.clearing.infrastructure.persistence.mapper.ReconciliationResultMapper;
import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/** 执行只读资金对账，并把发现的差异幂等落入 reconciliation_result。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ReconciliationApplicationService {

    private static final int BATCH_SIZE = 500;
    private static final int SETTLEMENT_GRACE_SECONDS = 300;

    private final ReconciliationReadMapper reconciliationReadMapper;
    private final ReconciliationResultMapper reconciliationResultMapper;
    private final ClearingMetrics clearingMetrics;
    private final ClearingAlertService clearingAlertService;

    /** 执行 Trade、Settlement/Ledger、Account/Ledger 三类对账并返回本轮发现数量。 */
    public ReconciliationSummary reconcile() {
        int tradeSettlementIssues = reconcileTradeSettlement();
        int settlementLedgerIssues = reconcileSettlementLedger();
        int accountLedgerIssues = reconcileAccountLedger();
        int total = tradeSettlementIssues + settlementLedgerIssues + accountLedgerIssues;
        if (total > 0) {
            clearingMetrics.recordReconciliationDifferences(total);
            clearingAlertService.alert("RECONCILIATION_DIFFERENCE", "batch", String.valueOf(total));
            log.error("清算对账发现差异: tradeSettlement={}, settlementLedger={}, accountLedger={}",
                    tradeSettlementIssues, settlementLedgerIssues, accountLedgerIssues);
        }
        return new ReconciliationSummary(tradeSettlementIssues, settlementLedgerIssues, accountLedgerIssues);
    }

    /** 针对单个历史差异重新读取资金事实，返回问题是否已经消失。 */
    public boolean isResolved(ReconciliationResultPO issue) {
        ReconciliationType type = ReconciliationType.valueOf(issue.getReconciliationType());
        if (type == ReconciliationType.TRADE_SETTLEMENT) {
            return reconciliationReadMapper.countSuccessfulSettlement(issue.getTradeId()) == 1;
        }
        if (type == ReconciliationType.SETTLEMENT_LEDGER) {
            return reconciliationReadMapper.countCompleteSettlementLedger(issue.getTradeId()) == 1;
        }
        AccountLedgerSnapshotRow row = reconciliationReadMapper
                .selectAccountLedgerInconsistency(issue.getUserId(), issue.getAsset());
        if (row == null) {
            return true;
        }
        return "AVAILABLE".equals(issue.getCheckItem())
                ? !isDifferent(row.getExpectedAvailable(), row.getActualAvailable())
                : !isDifferent(row.getExpectedFrozen(), row.getActualFrozen());
    }

    /** 对账清算已接收的成交快照与 SUCCESS 结算结果。 */
    private int reconcileTradeSettlement() {
        List<IncompleteSettlementRow> rows = reconciliationReadMapper.selectIncompleteSettlements(
                LocalDateTime.now().minusSeconds(SETTLEMENT_GRACE_SECONDS), BATCH_SIZE);
        for (IncompleteSettlementRow row : rows) {
            upsertIssue(ReconciliationType.TRADE_SETTLEMENT, row.getTradeId(), row.getTradeId(), null, null,
                    "SETTLEMENT_SUCCESS", BigDecimal.ONE, BigDecimal.ZERO,
                    "成交快照尚未结算成功，当前状态=" + row.getSettlementStatus());
        }
        return rows.size();
    }

    /** 对账每个 SUCCESS Settlement 是否拥有成功 Journal、Journal Entry 和余额流水。 */
    private int reconcileSettlementLedger() {
        List<IncompleteSettlementRow> rows = reconciliationReadMapper.selectSettlementLedgerInconsistencies(BATCH_SIZE);
        for (IncompleteSettlementRow row : rows) {
            upsertIssue(ReconciliationType.SETTLEMENT_LEDGER, row.getTradeId(), row.getTradeId(), null, null,
                    "JOURNAL_AND_FLOW", BigDecimal.ONE, BigDecimal.ZERO,
                    "结算成功但缺少 Journal、Journal Entry 或 Balance Flow");
        }
        return rows.size();
    }

    /** 对账账户当前可用/冻结余额与首笔流水期初加累计流水的重建结果。 */
    private int reconcileAccountLedger() {
        List<AccountLedgerSnapshotRow> rows = reconciliationReadMapper.selectAccountLedgerInconsistencies(BATCH_SIZE);
        int issues = 0;
        for (AccountLedgerSnapshotRow row : rows) {
            String accountBizId = row.getUserId() + ":" + row.getAsset();
            if (isDifferent(row.getExpectedAvailable(), row.getActualAvailable())) {
                upsertIssue(ReconciliationType.ACCOUNT_LEDGER, accountBizId, null, row.getUserId(), row.getAsset(),
                        "AVAILABLE", row.getExpectedAvailable(), row.getActualAvailable(), "账户可用余额与流水重建结果不一致");
                issues++;
            }
            if (isDifferent(row.getExpectedFrozen(), row.getActualFrozen())) {
                upsertIssue(ReconciliationType.ACCOUNT_LEDGER, accountBizId, null, row.getUserId(), row.getAsset(),
                        "FROZEN", row.getExpectedFrozen(), row.getActualFrozen(), "账户冻结余额与流水重建结果不一致");
                issues++;
            }
        }
        return issues;
    }

    /** 幂等保存单条差异；差异金额统一按 actual - expected 计算。 */
    private void upsertIssue(ReconciliationType type, String bizId, String tradeId, Long userId, String asset,
                             String checkItem, BigDecimal expected, BigDecimal actual, String errorMessage) {
        LocalDateTime now = LocalDateTime.now();
        ReconciliationResultPO result = new ReconciliationResultPO();
        result.setId(IdWorker.getId());
        result.setReconciliationType(type.name());
        result.setBizId(bizId);
        result.setTradeId(tradeId);
        result.setUserId(userId);
        result.setAsset(asset);
        result.setCheckItem(checkItem);
        result.setExpectedAmount(expected);
        result.setActualAmount(actual);
        result.setDifference(actual.subtract(expected));
        result.setErrorMessage(errorMessage);
        result.setCreatedAt(now);
        result.setUpdatedAt(now);
        reconciliationResultMapper.upsertOpen(result);
    }

    /** BigDecimal 比较必须使用 compareTo，避免数值相等但 scale 不同误报。 */
    private boolean isDifferent(BigDecimal expected, BigDecimal actual) {
        return expected == null || actual == null || expected.compareTo(actual) != 0;
    }

    /** 对账本轮汇总。 */
    public record ReconciliationSummary(int tradeSettlementIssues, int settlementLedgerIssues, int accountLedgerIssues) {
        public int totalIssues() {
            return tradeSettlementIssues + settlementLedgerIssues + accountLedgerIssues;
        }
    }
}
