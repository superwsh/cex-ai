package com.cex.clearing.infrastructure.persistence.mapper;

import com.cex.clearing.infrastructure.persistence.dto.AccountLedgerSnapshotRow;
import com.cex.clearing.infrastructure.persistence.dto.IncompleteSettlementRow;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.LocalDateTime;
import java.util.List;

/** 对账只读查询，禁止在此 Mapper 中修改账户、流水或凭证。 */
@Mapper
public interface ReconciliationReadMapper {

    /** 查找已超过宽限时间但尚未成功结算的成交快照。 */
    @Select("""
            SELECT trade_id AS tradeId, status AS settlementStatus
            FROM settlement_task
            WHERE status <> 'SUCCESS' AND created_at < #{before}
            ORDER BY created_at ASC, id ASC
            LIMIT #{limit}
            """)
    List<IncompleteSettlementRow> selectIncompleteSettlements(@Param("before") LocalDateTime before,
                                                               @Param("limit") int limit);

    /** 查找 SUCCESS 结算任务缺少成功凭证、凭证明细或余额流水的情况。 */
    @Select("""
            SELECT st.trade_id AS tradeId, st.status AS settlementStatus
            FROM settlement_task st
            WHERE st.status = 'SUCCESS'
              AND (
                  NOT EXISTS (SELECT 1 FROM settlement_journal sj
                              WHERE sj.trade_id = st.trade_id AND sj.status = 'SUCCESS')
                  OR NOT EXISTS (SELECT 1 FROM settlement_journal sj
                                 JOIN settlement_journal_entry se ON se.journal_id = sj.journal_id
                                 WHERE sj.trade_id = st.trade_id AND sj.status = 'SUCCESS')
                  OR NOT EXISTS (SELECT 1 FROM balance_flow bf
                                 WHERE bf.biz_type = 'TRADE_SETTLEMENT' AND bf.biz_id = st.trade_id)
              )
            ORDER BY st.settled_at ASC, st.id ASC
            LIMIT #{limit}
            """)
    List<IncompleteSettlementRow> selectSettlementLedgerInconsistencies(@Param("limit") int limit);

    /** 根据首笔流水的期初快照和累计变动重建账户余额，返回出现差额的账户。 */
    @Select("""
            SELECT ab.user_id AS userId, ab.asset AS asset,
                   (COALESCE((SELECT first_flow.available_before
                              FROM balance_flow first_flow
                              WHERE first_flow.user_id = ab.user_id AND first_flow.asset = ab.asset
                              ORDER BY first_flow.created_at ASC, first_flow.id ASC LIMIT 1), ab.available)
                    + COALESCE((SELECT SUM(available_change) FROM balance_flow flow
                                WHERE flow.user_id = ab.user_id AND flow.asset = ab.asset), 0)) AS expectedAvailable,
                   ab.available AS actualAvailable,
                   (COALESCE((SELECT first_flow.frozen_before
                              FROM balance_flow first_flow
                              WHERE first_flow.user_id = ab.user_id AND first_flow.asset = ab.asset
                              ORDER BY first_flow.created_at ASC, first_flow.id ASC LIMIT 1), ab.frozen)
                    + COALESCE((SELECT SUM(frozen_change) FROM balance_flow flow
                                WHERE flow.user_id = ab.user_id AND flow.asset = ab.asset), 0)) AS expectedFrozen,
                   ab.frozen AS actualFrozen
            FROM account_balance ab
            WHERE EXISTS (SELECT 1 FROM balance_flow flow
                          WHERE flow.user_id = ab.user_id AND flow.asset = ab.asset)
            HAVING expectedAvailable <> actualAvailable OR expectedFrozen <> actualFrozen
            ORDER BY ab.user_id ASC, ab.asset ASC
            LIMIT #{limit}
            """)
    List<AccountLedgerSnapshotRow> selectAccountLedgerInconsistencies(@Param("limit") int limit);

    /** 判断指定成交是否已经成功结算。 */
    @Select("SELECT COUNT(1) FROM settlement_task WHERE trade_id = #{tradeId} AND status = 'SUCCESS'")
    int countSuccessfulSettlement(@Param("tradeId") String tradeId);

    /** 判断指定成功结算是否同时拥有 Journal、Entry 和 Balance Flow。 */
    @Select("""
            SELECT COUNT(1)
            FROM settlement_task st
            WHERE st.trade_id = #{tradeId} AND st.status = 'SUCCESS'
              AND EXISTS (SELECT 1 FROM settlement_journal sj
                          WHERE sj.trade_id = st.trade_id AND sj.status = 'SUCCESS')
              AND EXISTS (SELECT 1 FROM settlement_journal sj
                          JOIN settlement_journal_entry se ON se.journal_id = sj.journal_id
                          WHERE sj.trade_id = st.trade_id AND sj.status = 'SUCCESS')
              AND EXISTS (SELECT 1 FROM balance_flow bf
                          WHERE bf.biz_type = 'TRADE_SETTLEMENT' AND bf.biz_id = st.trade_id)
            """)
    int countCompleteSettlementLedger(@Param("tradeId") String tradeId);

    /** 重新计算指定账户；仅在可用或冻结余额仍有差异时返回记录。 */
    @Select("""
            SELECT ab.user_id AS userId, ab.asset AS asset,
                   (COALESCE((SELECT first_flow.available_before FROM balance_flow first_flow
                              WHERE first_flow.user_id = ab.user_id AND first_flow.asset = ab.asset
                              ORDER BY first_flow.created_at ASC, first_flow.id ASC LIMIT 1), ab.available)
                    + COALESCE((SELECT SUM(available_change) FROM balance_flow flow
                                WHERE flow.user_id = ab.user_id AND flow.asset = ab.asset), 0)) AS expectedAvailable,
                   ab.available AS actualAvailable,
                   (COALESCE((SELECT first_flow.frozen_before FROM balance_flow first_flow
                              WHERE first_flow.user_id = ab.user_id AND first_flow.asset = ab.asset
                              ORDER BY first_flow.created_at ASC, first_flow.id ASC LIMIT 1), ab.frozen)
                    + COALESCE((SELECT SUM(frozen_change) FROM balance_flow flow
                                WHERE flow.user_id = ab.user_id AND flow.asset = ab.asset), 0)) AS expectedFrozen,
                   ab.frozen AS actualFrozen
            FROM account_balance ab
            WHERE ab.user_id = #{userId} AND ab.asset = #{asset}
            HAVING expectedAvailable <> actualAvailable OR expectedFrozen <> actualFrozen
            """)
    AccountLedgerSnapshotRow selectAccountLedgerInconsistency(@Param("userId") Long userId,
                                                               @Param("asset") String asset);
}
