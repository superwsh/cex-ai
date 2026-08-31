package com.cex.clearing.config;

import com.cex.clearing.domain.clearing.ClearingCalculator;
import com.cex.clearing.domain.clearing.FeeCalculator;
import com.cex.clearing.domain.clearing.TradeEventFeeCalculator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 清算纯领域对象装配，避免领域对象依赖 Spring 注解。 */
@Configuration
public class ClearingDomainConfiguration {

    /** 创建使用成交快照固定手续费的计算器。 */
    @Bean
    public FeeCalculator feeCalculator() {
        return new TradeEventFeeCalculator();
    }

    /** 创建无状态的成交清算计算器。 */
    @Bean
    public ClearingCalculator clearingCalculator(FeeCalculator feeCalculator) {
        return new ClearingCalculator(feeCalculator);
    }
}
