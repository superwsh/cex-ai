package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.persistence.entity.SymbolConfigPO;
import com.cex.order.infrastructure.persistence.mapper.SymbolConfigMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SymbolConfigRepository {

    private final SymbolConfigMapper symbolConfigMapper;

    public SymbolConfig findBySymbol(String symbol) {
        SymbolConfigPO po = symbolConfigMapper.selectOne(new LambdaQueryWrapper<SymbolConfigPO>()
                .eq(SymbolConfigPO::getSymbol, symbol));
        return po == null ? null : toDomain(po);
    }

    private SymbolConfig toDomain(SymbolConfigPO po) {
        return SymbolConfig.builder()
                .symbol(po.getSymbol()).baseCurrency(po.getBaseCurrency())
                .quoteCurrency(po.getQuoteCurrency()).priceScale(po.getPriceScale())
                .quantityScale(po.getQuantityScale()).minQuantity(po.getMinQuantity())
                .minAmount(po.getMinAmount())
                .status(SymbolConfig.SymbolStatus.valueOf(po.getStatus()))
                .build();
    }
}
