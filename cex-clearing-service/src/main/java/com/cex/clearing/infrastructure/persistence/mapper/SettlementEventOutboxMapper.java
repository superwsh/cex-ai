package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;

/** 结算发件箱 Mapper。 */
@Mapper
public interface SettlementEventOutboxMapper extends BaseMapper<SettlementEventOutboxPO> {

    /** 原子认领待投递记录；过期 SENDING 记录可被新实例恢复。 */
    @Update("""
            UPDATE settlement_event_outbox
            SET status = 'SENDING', processing_token = #{processingToken},
                next_retry_time = #{leaseUntil}, updated_at = #{now}
            WHERE id = #{id}
              AND status IN ('NEW', 'RETRY', 'SENDING')
              AND next_retry_time <= #{now}
              AND retry_count < #{maxRetry}
            """)
    int claimForPublish(@Param("id") Long id, @Param("processingToken") String processingToken,
                        @Param("leaseUntil") LocalDateTime leaseUntil, @Param("now") LocalDateTime now,
                        @Param("maxRetry") int maxRetry);

    /** 仅允许持有发送令牌的发布器将事件标记为已发布。 */
    @Update("""
            UPDATE settlement_event_outbox
            SET status = 'PUBLISHED', published_at = #{now}, updated_at = #{now},
                processing_token = NULL, last_error = NULL
            WHERE id = #{id} AND status = 'SENDING' AND processing_token = #{processingToken}
            """)
    int markPublished(@Param("id") Long id, @Param("processingToken") String processingToken,
                      @Param("now") LocalDateTime now);

    /** 仅允许持有发送令牌的发布器登记失败并安排下一次重试。 */
    @Update("""
            UPDATE settlement_event_outbox
            SET status = #{status}, retry_count = #{retryCount}, next_retry_time = #{nextRetryTime},
                last_error = #{lastError}, processing_token = NULL, updated_at = #{now}
            WHERE id = #{id} AND status = 'SENDING' AND processing_token = #{processingToken}
            """)
    int markRetry(@Param("id") Long id, @Param("processingToken") String processingToken,
                  @Param("status") String status, @Param("retryCount") int retryCount,
                  @Param("nextRetryTime") LocalDateTime nextRetryTime, @Param("lastError") String lastError,
                  @Param("now") LocalDateTime now);
}
