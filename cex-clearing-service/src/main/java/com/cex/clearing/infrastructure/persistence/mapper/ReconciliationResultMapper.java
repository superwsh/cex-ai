package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.ReconciliationResultPO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

/** 对账差异结果持久化。 */
@Mapper
public interface ReconciliationResultMapper extends BaseMapper<ReconciliationResultPO> {

    /** 幂等写入或重新打开同一业务差异，避免定时扫描重复制造问题记录。 */
    @Insert("""
            INSERT INTO reconciliation_result
            (id, reconciliation_type, biz_id, trade_id, user_id, asset, check_item,
             expected_amount, actual_amount, difference, status, error_message, created_at, updated_at)
            VALUES (#{id}, #{reconciliationType}, #{bizId}, #{tradeId}, #{userId}, #{asset}, #{checkItem},
                    #{expectedAmount}, #{actualAmount}, #{difference}, 'OPEN', #{errorMessage}, #{createdAt}, #{updatedAt})
            ON DUPLICATE KEY UPDATE
                trade_id = VALUES(trade_id), user_id = VALUES(user_id), asset = VALUES(asset),
                expected_amount = VALUES(expected_amount), actual_amount = VALUES(actual_amount),
                difference = VALUES(difference), status = 'OPEN', error_message = VALUES(error_message),
                resolved_at = NULL, updated_at = VALUES(updated_at)
            """)
    int upsertOpen(ReconciliationResultPO result);
}
