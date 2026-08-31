package com.cex.order.infrastructure.asset;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 资产冻结 Mock 实现(Phase 1)
 * 内存账本模拟可用余额/冻结余额,支持 bizType+bizId 幂等。
 * TODO: 资产服务就绪后替换为 Dubbo 实现,删除本类。
 */
@Slf4j
public class MockAccountServiceClient implements AccountServiceClient {

    /** userId:currency -> 可用余额 */
    private final Map<String, BigDecimal> available = new ConcurrentHashMap<>();
    /** userId:currency -> 冻结余额 */
    private final Map<String, BigDecimal> frozen = new ConcurrentHashMap<>();
    /** bizType:bizId -> 已冻结金额(幂等记录) */
    private final Set<String> frozenRecords = ConcurrentHashMap.newKeySet();

    public MockAccountServiceClient() {
        // 预置测试账户余额:用户 100
        available.put("100:USDT", new BigDecimal("1000000"));
        available.put("100:BTC", new BigDecimal("100"));
        available.put("100:ETH", new BigDecimal("1000"));
    }

    /**
     * 测试专用构造器:自定义初始余额(map key 为 "userId:currency")
     */
    public MockAccountServiceClient(Map<String, BigDecimal> initialBalances) {
        initialBalances.forEach((k, v) -> available.put(k, v));
    }

    @Override
    public synchronized void freeze(FreezeRequest request) {
        String key = request.getUserId() + ":" + request.getCurrency();
        String recordKey = request.getBizType() + ":" + request.getBizId();
        if (frozenRecords.contains(recordKey)) {
            log.info("[MOCK] 冻结幂等命中,跳过: {}", recordKey);
            return;
        }
        BigDecimal avail = available.getOrDefault(key, BigDecimal.ZERO);
        if (avail.compareTo(request.getAmount()) < 0) {
            throw new BizException(ErrorCode.INSUFFICIENT_BALANCE.getCode(),
                    ErrorCode.INSUFFICIENT_BALANCE.getMessage() + ": " + request.getCurrency());
        }
        available.put(key, avail.subtract(request.getAmount()));
        frozen.put(key, frozen.getOrDefault(key, BigDecimal.ZERO).add(request.getAmount()));
        frozenRecords.add(recordKey);
        log.info("[MOCK] 冻结成功: userId={}, currency={}, amount={}, bizType={}, bizId={}",
                request.getUserId(), request.getCurrency(), request.getAmount(),
                request.getBizType(), request.getBizId());
    }

    @Override
    public synchronized void unfreeze(UnfreezeRequest request) {
        String key = request.getUserId() + ":" + request.getCurrency();
        BigDecimal fz = frozen.getOrDefault(key, BigDecimal.ZERO);
        if (fz.compareTo(request.getAmount()) < 0) {
            throw new BizException(ErrorCode.FREEZE_FAILED.getCode(),
                    "解冻金额超过冻结金额: " + request.getAmount() + " > " + fz);
        }
        frozen.put(key, fz.subtract(request.getAmount()));
        available.put(key, available.getOrDefault(key, BigDecimal.ZERO).add(request.getAmount()));
        log.info("[MOCK] 解冻成功: userId={}, currency={}, amount={}, bizType={}, bizId={}",
                request.getUserId(), request.getCurrency(), request.getAmount(),
                request.getBizType(), request.getBizId());
    }

    /** 测试辅助:获取可用余额 */
    public BigDecimal getAvailable(Long userId, String currency) {
        return available.getOrDefault(userId + ":" + currency, BigDecimal.ZERO);
    }

    /** 测试辅助:获取冻结余额 */
    public BigDecimal getFrozen(Long userId, String currency) {
        return frozen.getOrDefault(userId + ":" + currency, BigDecimal.ZERO);
    }
}
