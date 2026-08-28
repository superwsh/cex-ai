package com.cex.user;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 用户服务启动类
 * scanBasePackages 覆盖 com.cex 下所有公共模块配置（web/redis/mysql/kafka/dubbo）
 */
@EnableDubbo
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexUserServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexUserServiceApplication.class, args);
    }
}
