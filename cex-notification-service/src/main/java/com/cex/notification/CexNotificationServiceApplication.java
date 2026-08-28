package com.cex.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 通知服务启动类
 * 核心链路：消费 TradeEvent / AssetChangeEvent（Kafka）-> 按 userId 定向 WebSocket 推送
 */
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexNotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexNotificationServiceApplication.class, args);
    }
}
