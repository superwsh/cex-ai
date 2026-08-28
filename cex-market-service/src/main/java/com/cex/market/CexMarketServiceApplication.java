package com.cex.market;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 行情服务启动类
 * 核心链路：消费 TradeEvent（Kafka）-> 聚合 tick/K 线 -> Redis 缓存 -> Netty WebSocket 推送
 */
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexMarketServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexMarketServiceApplication.class, args);
    }
}
