package com.cex.clearing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 清算后台接口鉴权配置；令牌只能通过外部配置注入。 */
@Data
@Component
@ConfigurationProperties(prefix = "clearing.admin")
public class ClearingAdminProperties {
    private String token;
}
