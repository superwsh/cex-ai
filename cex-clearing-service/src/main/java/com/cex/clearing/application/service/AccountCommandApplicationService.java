package com.cex.clearing.application.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.clearing.common.ClearingErrorCode;
import com.cex.clearing.infrastructure.persistence.entity.AccountBalancePO;
import com.cex.clearing.infrastructure.persistence.entity.AccountOperationPO;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import com.cex.clearing.infrastructure.persistence.mapper.AccountBalanceMapper;
import com.cex.clearing.infrastructure.persistence.mapper.AccountOperationMapper;
import com.cex.clearing.infrastructure.persistence.mapper.BalanceFlowMapper;
import com.cex.common.core.exception.BizException;
import com.cex.common.dubbo.account.FreezeBalanceRequest;
import com.cex.common.dubbo.account.UnfreezeBalanceRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 账户冻结与解冻应用服务。
 * 余额原子更新、幂等记录及流水始终在同一数据库事务中完成。
 */
@Service
@RequiredArgsConstructor
public class AccountCommandApplicationService {

    private static final String FREEZE = "FREEZE";
    private static final String UNFREEZE = "UNFREEZE";

    private final AccountBalanceMapper accountBalanceMapper;
    private final AccountOperationMapper accountOperationMapper;
    private final BalanceFlowMapper balanceFlowMapper;

    /** 执行可用余额冻结。 */
    @Transactional
    public void freeze(FreezeBalanceRequest request) {
        validate(request.userId(), request.asset(), request.amount(), request.bizType(), request.bizId());
        if (!registerOperation(request.userId(), request.asset(), request.bizType(), request.bizId(), FREEZE)) {
            return;
        }
        if (accountBalanceMapper.freeze(request.userId(), request.asset(), request.amount()) != 1) {
            throw exception(ClearingErrorCode.INSUFFICIENT_AVAILABLE_BALANCE);
        }
        insertFlow(request.userId(), request.asset(), request.amount(), request.bizType(), request.bizId(), FREEZE);
    }

    /** 执行冻结余额解冻。 */
    @Transactional
    public void unfreeze(UnfreezeBalanceRequest request) {
        validate(request.userId(), request.asset(), request.amount(), request.bizType(), request.bizId());
        if (!registerOperation(request.userId(), request.asset(), request.bizType(), request.bizId(), UNFREEZE)) {
            return;
        }
        if (accountBalanceMapper.unfreeze(request.userId(), request.asset(), request.amount()) != 1) {
            throw exception(ClearingErrorCode.INSUFFICIENT_FROZEN_BALANCE);
        }
        insertFlow(request.userId(), request.asset(), request.amount(), request.bizType(), request.bizId(), UNFREEZE);
    }

    /** 注册业务命令；唯一索引冲突说明同一命令已经成功提交。 */
    private boolean registerOperation(Long userId, String asset, String bizType, String bizId, String operationType) {
        try {
            AccountOperationPO operation = new AccountOperationPO();
            operation.setBizType(bizType);
            operation.setBizId(bizId);
            operation.setOperationType(operationType);
            operation.setUserId(userId);
            operation.setAsset(asset);
            operation.setCreatedAt(LocalDateTime.now());
            accountOperationMapper.insert(operation);
            return true;
        } catch (DuplicateKeyException exception) {
            return false;
        }
    }

    /** 读取更新后的余额并写入不可变流水。 */
    private void insertFlow(Long userId, String asset, BigDecimal amount, String bizType, String bizId, String operationType) {
        AccountBalancePO balance = accountBalanceMapper.selectOne(new LambdaQueryWrapper<AccountBalancePO>()
                .eq(AccountBalancePO::getUserId, userId).eq(AccountBalancePO::getAsset, asset));
        if (balance == null) {
            throw exception(ClearingErrorCode.INVALID_ACCOUNT_COMMAND);
        }
        boolean freeze = FREEZE.equals(operationType);
        BalanceFlowPO flow = new BalanceFlowPO();
        flow.setBizType(bizType);
        flow.setBizId(bizId);
        flow.setFlowType(freeze ? "ORDER_FREEZE" : "ORDER_UNFREEZE");
        flow.setUserId(userId);
        flow.setAsset(asset);
        flow.setAvailableAfter(balance.getAvailable());
        flow.setFrozenAfter(balance.getFrozen());
        flow.setAvailableChange(freeze ? amount.negate() : amount);
        flow.setFrozenChange(freeze ? amount : amount.negate());
        flow.setAvailableBefore(balance.getAvailable().subtract(flow.getAvailableChange()));
        flow.setFrozenBefore(balance.getFrozen().subtract(flow.getFrozenChange()));
        flow.setCreatedAt(LocalDateTime.now());
        balanceFlowMapper.insert(flow);
    }

    /** 校验账户资金指令。 */
    private void validate(Long userId, String asset, BigDecimal amount, String bizType, String bizId) {
        if (userId == null || userId <= 0 || asset == null || asset.isBlank() || amount == null
                || amount.signum() <= 0 || bizType == null || bizType.isBlank() || bizId == null || bizId.isBlank()) {
            throw exception(ClearingErrorCode.INVALID_ACCOUNT_COMMAND);
        }
    }

    private BizException exception(ClearingErrorCode errorCode) {
        return new BizException(errorCode.getCode(), errorCode.getMessage());
    }
}
