package com.cex.clearing;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 清算服务启动类
 * 核心链路：消费 TradeEvent（Kafka）-> 账户记账/流水入账（Account + Ledger）-> 广播资产变更
 */
@EnableDubbo
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexClearingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexClearingServiceApplication.class, args);
    }
}
