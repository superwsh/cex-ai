package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.redis.SymbolConfigCache;
import com.cex.order.infrastructure.repository.SymbolConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * 交易对配置服务:Redis 缓存优先,DB 兜底并回填
 */
@Service
@RequiredArgsConstructor
public class SymbolConfigService {

    private final SymbolConfigRepository repository;
    private final SymbolConfigCache cache;

    /**
     * 获取可交易交易对配置;不存在或暂停直接抛业务异常
     */
    public SymbolConfig getRequired(String symbol) {
        SymbolConfig config = cache.get(symbol);
        if (config == null) {
            config = repository.findBySymbol(symbol);
            if (config == null) {
                throw new BizException(ErrorCode.SYMBOL_NOT_FOUND.getCode(),
                        ErrorCode.SYMBOL_NOT_FOUND.getMessage() + ": " + symbol);
            }
            cache.put(symbol, config);
        }
        if (!config.isTradable()) {
            throw new BizException(ErrorCode.SYMBOL_PAUSED.getCode(),
                    ErrorCode.SYMBOL_PAUSED.getMessage() + ": " + symbol);
        }
        return config;
    }
}
