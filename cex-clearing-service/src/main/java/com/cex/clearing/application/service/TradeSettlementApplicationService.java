package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.clearing.domain.clearing.AccountPosting;
import com.cex.clearing.domain.clearing.ClearingCalculator;
import com.cex.clearing.domain.clearing.ClearingResult;
import com.cex.clearing.domain.clearing.PostingType;
import com.cex.clearing.domain.settlement.SettlementException;
import com.cex.clearing.infrastructure.persistence.entity.AccountBalancePO;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalEntryPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalPO;
import com.cex.clearing.infrastructure.persistence.mapper.AccountBalanceMapper;
import com.cex.clearing.infrastructure.persistence.mapper.BalanceFlowMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementEventOutboxMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalEntryMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.common.kafka.event.TradeSettledEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 成交结算核心事务。
 * 账户变更、资金流水、Journal、结算状态及 Outbox 必须在同一事务提交。
 */
@Service
@RequiredArgsConstructor
public class TradeSettlementApplicationService {

    private static final String BIZ_TYPE = "TRADE_SETTLEMENT";
    private static final String JOURNAL_PREFIX = "trade-journal-";

    private final SettlementTaskMapper settlementTaskMapper;
    private final AccountBalanceMapper accountBalanceMapper;
    private final BalanceFlowMapper balanceFlowMapper;
    private final SettlementJournalMapper journalMapper;
    private final SettlementJournalEntryMapper journalEntryMapper;
    private final SettlementEventOutboxMapper outboxMapper;
    private final ClearingCalculator clearingCalculator;
    private final ObjectMapper objectMapper;

    /**
     * 执行一笔已建档成交的原子结算。
     *
     * @param event 已通过 Phase 3 完整性校验并已建档的成交事件
     */
    @Transactional
    public boolean settle(TradeEvent event) {
        ClearingResult clearingResult = clearingCalculator.calculate(event);
        if (!claimTask(event.getTradeId())) {
            return false;
        }

        Map<AccountKey, AccountBalancePO> lockedBalances = lockRelatedAccounts(clearingResult);
        String journalId = createJournal(event.getTradeId());
        for (AccountPosting posting : clearingResult.postings()) {
            applyPosting(posting, event.getTradeId(), lockedBalances);
            writeJournalEntry(journalId, posting);
        }
        markJournalSuccess(journalId);
        if (settlementTaskMapper.markSuccess(event.getTradeId()) != 1) {
            throw retryable("SETTLEMENT_STATUS_CONFLICT", "结算任务状态更新冲突", null);
        }
        writeOutbox(event);
        return true;
    }

    /** 使用状态机 CAS 认领任务；重复消息在已成功或处理中时不再重复入账。 */
    private boolean claimTask(String tradeId) {
        try {
            return settlementTaskMapper.claimForSettlement(tradeId) == 1;
        } catch (DataAccessException exception) {
            throw retryable("SETTLEMENT_TASK_LOCK_FAILED", "结算任务认领失败", exception);
        }
    }

    /** 按账户 ID 升序锁住本次所有账户，并构建连续过账使用的余额快照。 */
    private Map<AccountKey, AccountBalancePO> lockRelatedAccounts(ClearingResult clearingResult) {
        List<AccountKey> accountKeys = clearingResult.postings().stream()
                .map(posting -> new AccountKey(posting.userId(), posting.asset()))
                .distinct().sorted(Comparator.comparing(AccountKey::userId).thenComparing(AccountKey::asset)).toList();
        List<AccountBalancePO> balances = accountBalanceMapper.selectList(buildAccountLockQuery(accountKeys));
        if (balances.size() != accountKeys.size()) {
            throw permanent("SETTLEMENT_ACCOUNT_NOT_FOUND", "结算账户不存在");
        }
        Map<AccountKey, AccountBalancePO> lockedBalances = new HashMap<>();
        for (AccountBalancePO balance : balances) {
            lockedBalances.put(new AccountKey(balance.getUserId(), balance.getAsset()), balance);
        }
        if (lockedBalances.size() != accountKeys.size()) {
            throw permanent("SETTLEMENT_ACCOUNT_NOT_FOUND", "结算账户不存在");
        }
        return lockedBalances;
    }

    /** 构造账户行锁查询；账户键来自内部清算结果，不接受外部 SQL 片段。 */
    private LambdaQueryWrapper<AccountBalancePO> buildAccountLockQuery(List<AccountKey> accountKeys) {
        return new LambdaQueryWrapper<AccountBalancePO>().and(wrapper -> {
            for (int index = 0; index < accountKeys.size(); index++) {
                AccountKey key = accountKeys.get(index);
                if (index > 0) {
                    wrapper.or();
                }
                wrapper.eq(AccountBalancePO::getUserId, key.userId()).eq(AccountBalancePO::getAsset, key.asset());
            }
        }).orderByAsc(AccountBalancePO::getId).last("FOR UPDATE");
    }

    /** 原子应用余额变化，并以变更前后快照生成不可变资金流水。 */
    private void applyPosting(AccountPosting posting, String tradeId, Map<AccountKey, AccountBalancePO> lockedBalances) {
        AccountBalancePO balance = lockedBalances.get(new AccountKey(posting.userId(), posting.asset()));
        if (balance == null) {
            throw permanent("SETTLEMENT_ACCOUNT_NOT_FOUND", "结算账户不存在");
        }
        BalanceSnapshot before = BalanceSnapshot.of(balance);
        int affectedRows = accountBalanceMapper.applyChange(posting.userId(), posting.asset(),
                posting.availableChange(), posting.frozenChange());
        if (affectedRows != 1) {
            throw permanent("INSUFFICIENT_SETTLEMENT_BALANCE", "冻结余额不足或账户状态冲突");
        }
        balance.setAvailable(before.available().add(posting.availableChange()));
        balance.setFrozen(before.frozen().add(posting.frozenChange()));
        writeBalanceFlow(tradeId, posting, before, balance);
    }

    /** 写入与余额原子变更一一对应的审计流水。 */
    private void writeBalanceFlow(String tradeId, AccountPosting posting, BalanceSnapshot before, AccountBalancePO after) {
        BalanceFlowPO flow = new BalanceFlowPO();
        flow.setBizType(BIZ_TYPE);
        flow.setBizId(tradeId);
        flow.setFlowType(posting.type().name());
        flow.setUserId(posting.userId());
        flow.setAsset(posting.asset());
        flow.setAvailableBefore(before.available());
        flow.setAvailableChange(posting.availableChange());
        flow.setAvailableAfter(after.getAvailable());
        flow.setFrozenBefore(before.frozen());
        flow.setFrozenChange(posting.frozenChange());
        flow.setFrozenAfter(after.getFrozen());
        flow.setCreatedAt(LocalDateTime.now());
        if (balanceFlowMapper.insert(flow) != 1) {
            throw retryable("BALANCE_FLOW_WRITE_FAILED", "资金流水写入失败", null);
        }
    }

    /** 创建本成交唯一的 Journal 凭证头。 */
    private String createJournal(String tradeId) {
        LocalDateTime now = LocalDateTime.now();
        String journalId = JOURNAL_PREFIX + tradeId;
        SettlementJournalPO journal = new SettlementJournalPO();
        journal.setJournalId(journalId);
        journal.setBizType(BIZ_TYPE);
        journal.setBizId(tradeId);
        journal.setTradeId(tradeId);
        journal.setStatus("PROCESSING");
        journal.setCreatedAt(now);
        journal.setUpdatedAt(now);
        if (journalMapper.insert(journal) != 1) {
            throw retryable("JOURNAL_WRITE_FAILED", "结算凭证写入失败", null);
        }
        return journalId;
    }

    /** 写入对应账户过账的 Journal 分录。 */
    private void writeJournalEntry(String journalId, AccountPosting posting) {
        BigDecimal change = posting.availableChange().signum() != 0
                ? posting.availableChange() : posting.frozenChange();
        SettlementJournalEntryPO entry = new SettlementJournalEntryPO();
        entry.setJournalId(journalId);
        entry.setUserId(posting.userId());
        entry.setAsset(posting.asset());
        entry.setAccountType(accountType(posting));
        entry.setAmount(change.abs());
        entry.setDirection(change.signum() < 0 ? "DEBIT" : "CREDIT");
        entry.setCreatedAt(LocalDateTime.now());
        if (journalEntryMapper.insert(entry) != 1) {
            throw retryable("JOURNAL_ENTRY_WRITE_FAILED", "结算凭证明细写入失败", null);
        }
    }

    /** 根据过账类型识别 Journal 内部账户。 */
    private String accountType(AccountPosting posting) {
        if (posting.type() == PostingType.PLATFORM_FEE_CREDIT) {
            return "PLATFORM_FEE";
        }
        return posting.frozenChange().signum() != 0 ? "USER_FROZEN" : "USER_AVAILABLE";
    }

    /** 将凭证状态置为成功；失败则整体事务回滚。 */
    private void markJournalSuccess(String journalId) {
        SettlementJournalPO journal = new SettlementJournalPO();
        journal.setJournalId(journalId);
        journal.setStatus("SUCCESS");
        journal.setUpdatedAt(LocalDateTime.now());
        if (journalMapper.update(journal, new LambdaQueryWrapper<SettlementJournalPO>()
                .eq(SettlementJournalPO::getJournalId, journalId).eq(SettlementJournalPO::getStatus, "PROCESSING")) != 1) {
            throw retryable("JOURNAL_STATUS_CONFLICT", "结算凭证状态更新冲突", null);
        }
    }

    /** 在余额、流水、Journal 与任务状态均成功后插入待投递事件。 */
    private void writeOutbox(TradeEvent event) {
        LocalDateTime now = LocalDateTime.now();
        TradeSettledEvent settled = TradeSettledEvent.builder().eventId("trade-settled-" + event.getTradeId())
                .tradeId(event.getTradeId()).symbol(event.getSymbol()).buyOrderId(event.getBuyOrderId())
                .sellOrderId(event.getSellOrderId()).price(event.getPrice()).quantity(event.getQuantity())
                .amount(event.getAmount()).buyerFee(event.getBuyerFee()).buyerFeeAsset(event.getBuyerFeeAsset())
                .sellerFee(event.getSellerFee()).sellerFeeAsset(event.getSellerFeeAsset())
                .settledAt(System.currentTimeMillis()).build();
        SettlementEventOutboxPO outbox = new SettlementEventOutboxPO();
        outbox.setEventId(settled.getEventId());
        outbox.setAggregateId(event.getTradeId());
        outbox.setTopic(TopicConstants.TOPIC_TRADE_SETTLED_EVENT);
        outbox.setPayload(toJson(settled));
        outbox.setStatus("NEW");
        outbox.setRetryCount(0);
        outbox.setNextRetryTime(now);
        outbox.setProcessingToken(null);
        outbox.setLastError(null);
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        if (outboxMapper.insert(outbox) != 1) {
            throw retryable("OUTBOX_WRITE_FAILED", "结算事件 Outbox 写入失败", null);
        }
    }

    /** 序列化 Outbox 载荷。 */
    private String toJson(TradeSettledEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw retryable("OUTBOX_SERIALIZE_FAILED", "结算事件序列化失败", exception);
        }
    }

    /** 创建无需自动重试的业务失败。 */
    private SettlementException permanent(String errorCode, String message) {
        return new SettlementException(errorCode, message, false);
    }

    /** 创建需要由 Kafka 重试的基础设施失败。 */
    private SettlementException retryable(String errorCode, String message, Throwable cause) {
        return new SettlementException(errorCode, message, true, cause);
    }

    private record AccountKey(Long userId, String asset) {
    }

    private record BalanceSnapshot(BigDecimal available, BigDecimal frozen) {
        static BalanceSnapshot of(AccountBalancePO balance) {
            return new BalanceSnapshot(balance.getAvailable(), balance.getFrozen());
        }
    }
}
