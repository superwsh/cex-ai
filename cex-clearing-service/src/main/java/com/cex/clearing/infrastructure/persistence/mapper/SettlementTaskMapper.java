package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

/** 结算任务 Mapper。 */
@Mapper
public interface SettlementTaskMapper extends BaseMapper<SettlementTaskPO> {

    /** 使用状态机 CAS 认领 INIT 或 RETRY 任务，避免重复消息并发结算。 */
    @Update("UPDATE settlement_task SET status = 'PROCESSING', processing_at = CURRENT_TIMESTAMP, "
            + "updated_at = CURRENT_TIMESTAMP WHERE trade_id = #{tradeId} AND status IN ('INIT', 'RETRY')")
    int claimForSettlement(@Param("tradeId") String tradeId);

    /** 将已由当前事务认领的任务置为结算成功。 */
    @Update("UPDATE settlement_task SET status = 'SUCCESS', settled_at = CURRENT_TIMESTAMP, processing_at = NULL, "
            + "next_retry_time = NULL, error_code = NULL, error_message = NULL, updated_at = CURRENT_TIMESTAMP "
            + "WHERE trade_id = #{tradeId} AND status = 'PROCESSING'")
    int markSuccess(@Param("tradeId") String tradeId);

    /** 记录一次可恢复失败；达到重试上限时自动转人工复核。 */
    @Update("""
            UPDATE settlement_task
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= #{maxRetry} THEN 'MANUAL_REVIEW' ELSE 'RETRY' END,
                next_retry_time = CASE WHEN retry_count + 1 >= #{maxRetry} THEN NULL ELSE #{nextRetryTime} END,
                processing_at = NULL, error_code = #{errorCode}, error_message = #{errorMessage},
                updated_at = #{now}
            WHERE trade_id = #{tradeId} AND status IN ('INIT', 'RETRY', 'PROCESSING')
            """)
    int scheduleRetry(@Param("tradeId") String tradeId, @Param("maxRetry") int maxRetry,
                      @Param("nextRetryTime") LocalDateTime nextRetryTime, @Param("errorCode") String errorCode,
                      @Param("errorMessage") String errorMessage, @Param("now") LocalDateTime now);

    /** 将超过处理租约的任务恢复为 RETRY；上限耗尽的任务必须人工复核。 */
    @Update("""
            UPDATE settlement_task
            SET retry_count = retry_count + 1,
                status = CASE WHEN retry_count + 1 >= #{maxRetry} THEN 'MANUAL_REVIEW' ELSE 'RETRY' END,
                next_retry_time = CASE WHEN retry_count + 1 >= #{maxRetry} THEN NULL ELSE #{now} END,
                processing_at = NULL, error_code = 'PROCESSING_TIMEOUT',
                error_message = 'PROCESSING_TIMEOUT: 结算处理超时，已由恢复任务接管', updated_at = #{now}
            WHERE status = 'PROCESSING' AND processing_at < #{timeoutBefore}
            """)
    int recoverExpiredProcessing(@Param("timeoutBefore") LocalDateTime timeoutBefore,
                                 @Param("maxRetry") int maxRetry, @Param("now") LocalDateTime now);

    /** 恢复建档后因进程异常未进入结算事务的 INIT 任务。 */
    @Update("""
            UPDATE settlement_task
            SET status = 'RETRY', next_retry_time = #{now}, error_code = 'INIT_TIMEOUT',
                error_message = 'INIT_TIMEOUT: 建档后未进入结算，已由恢复任务接管', updated_at = #{now}
            WHERE status = 'INIT' AND created_at < #{timeoutBefore}
            """)
    int recoverExpiredInit(@Param("timeoutBefore") LocalDateTime timeoutBefore, @Param("now") LocalDateTime now);

    /** 按到期时间读取可重试任务，恢复服务必须使用持久化事件快照重建成交事件。 */
    @Select("""
            SELECT id, trade_id, event_id, symbol, buy_order_id, sell_order_id, buyer_user_id, seller_user_id,
                   base_asset, quote_asset, price, quantity, quote_amount, buyer_fee, buyer_fee_asset,
                   seller_fee, seller_fee_asset, match_sequence, trade_time, status, retry_count,
                   next_retry_time, processing_at, error_code, error_message, created_at, settled_at, updated_at
            FROM settlement_task
            WHERE status = 'RETRY' AND next_retry_time <= #{now}
            ORDER BY next_retry_time ASC, id ASC
            LIMIT #{limit}
            """)
    List<SettlementTaskPO> selectDueRetryTasks(@Param("now") LocalDateTime now, @Param("limit") int limit);

    /** 人工复核后仅把失败任务重新排入 RETRY，不在 HTTP 请求中直接执行资金结算。 */
    @Update("""
            UPDATE settlement_task
            SET status = 'RETRY', next_retry_time = #{now}, processing_at = NULL,
                error_code = NULL, error_message = NULL, updated_at = #{now}
            WHERE trade_id = #{tradeId} AND status IN ('MANUAL_REVIEW', 'FAILED')
            """)
    int scheduleManualRetry(@Param("tradeId") String tradeId, @Param("now") LocalDateTime now);
}
