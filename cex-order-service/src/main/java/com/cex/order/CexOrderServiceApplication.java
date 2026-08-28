package com.cex.order;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 订单服务启动类
 * 核心链路：下单（本地事务 + Outbox 表）-> 发布 OrderEvent 到 Kafka -> Matching Engine 撮合
 */
@EnableDubbo
@EnableScheduling
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexOrderServiceApplication.class, args);
    }
}
