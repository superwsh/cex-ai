package com.cex.matching;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 撮合引擎启动类
 * 核心链路：消费 OrderEvent（Kafka）-> 内存订单簿撮合 -> 发布 TradeEvent（Kafka）
 * 无数据库依赖，热状态驻留内存；启动时通过本地 WAL、订单簿快照与 Kafka 重投幂等结果恢复。
 */
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexMatchingEngineApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexMatchingEngineApplication.class, args);
    }
}
