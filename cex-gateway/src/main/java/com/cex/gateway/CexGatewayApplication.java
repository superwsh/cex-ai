package com.cex.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * API 网关启动类
 * 职责：统一入口 / 路由转发 / 认证鉴权 / 限流 / 跨域
 */
@SpringBootApplication
public class CexGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexGatewayApplication.class, args);
    }
}
