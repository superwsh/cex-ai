package com.cex.clearing.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.clearing.infrastructure.persistence.entity.AccountBalancePO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

import java.math.BigDecimal;

/** 余额原子更新 SQL。 */
@Mapper
public interface AccountBalanceMapper extends BaseMapper<AccountBalancePO> {

    /** 原子冻结：余额不足时返回 0。 */
    @Update("UPDATE account_balance SET available = available - #{amount}, frozen = frozen + #{amount}, "
            + "version = version + 1, updated_at = CURRENT_TIMESTAMP "
            + "WHERE user_id = #{userId} AND asset = #{asset} AND available >= #{amount}")
    int freeze(@Param("userId") Long userId, @Param("asset") String asset, @Param("amount") BigDecimal amount);

    /** 原子解冻：冻结余额不足时返回 0。 */
    @Update("UPDATE account_balance SET frozen = frozen - #{amount}, available = available + #{amount}, "
            + "version = version + 1, updated_at = CURRENT_TIMESTAMP "
            + "WHERE user_id = #{userId} AND asset = #{asset} AND frozen >= #{amount}")
    int unfreeze(@Param("userId") Long userId, @Param("asset") String asset, @Param("amount") BigDecimal amount);

    /**
     * 原子应用一笔清算过账；任一余额将变为负数时返回 0。
     *
     * @param userId 用户或平台账户 ID
     * @param asset 资产代码
     * @param availableChange 可用余额变动
     * @param frozenChange 冻结余额变动
     * @return 成功更新的记录数
     */
    @Update("UPDATE account_balance SET available = available + #{availableChange}, "
            + "frozen = frozen + #{frozenChange}, version = version + 1, updated_at = CURRENT_TIMESTAMP "
            + "WHERE user_id = #{userId} AND asset = #{asset} "
            + "AND available + #{availableChange} >= 0 AND frozen + #{frozenChange} >= 0")
    int applyChange(@Param("userId") Long userId, @Param("asset") String asset,
                    @Param("availableChange") BigDecimal availableChange,
                    @Param("frozenChange") BigDecimal frozenChange);
}
