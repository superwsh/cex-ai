package com.cex.clearing.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** 清算服务后台接口安全配置。 */
@Configuration
@RequiredArgsConstructor
public class ClearingWebMvcConfiguration implements WebMvcConfigurer {

    private final ClearingAdminAuthorizationInterceptor adminAuthorizationInterceptor;

    /** 所有清算后台接口统一经过鉴权拦截器。 */
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(adminAuthorizationInterceptor).addPathPatterns("/api/admin/clearing/**");
    }
}
