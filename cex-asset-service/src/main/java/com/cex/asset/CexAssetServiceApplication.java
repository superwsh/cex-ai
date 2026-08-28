package com.cex.asset;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 资产服务启动类
 * 核心链路：消费 TradeEvent -> 锁定/释放资产 -> 资金流水（Ledger）落库 -> 广播 AssetChangeEvent
 */
@EnableDubbo
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexAssetServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexAssetServiceApplication.class, args);
    }
}
