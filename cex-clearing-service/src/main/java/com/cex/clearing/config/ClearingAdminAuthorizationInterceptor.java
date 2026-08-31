package com.cex.clearing.config;

import com.cex.clearing.common.AdminAuthorizationException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** 对清算后台接口执行默认拒绝的令牌和操作人校验。 */
@Component
@RequiredArgsConstructor
public class ClearingAdminAuthorizationInterceptor implements HandlerInterceptor {

    public static final String TOKEN_HEADER = "X-CEX-Admin-Token";
    public static final String OPERATOR_HEADER = "X-CEX-Operator-Id";

    private final ClearingAdminProperties properties;

    /** 未配置令牌、令牌不匹配或缺少操作人时拒绝请求。 */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String expectedToken = properties.getToken();
        if (expectedToken == null || expectedToken.isBlank()) {
            throw new AdminAuthorizationException(HttpStatus.SERVICE_UNAVAILABLE, 50301, "清算后台鉴权未配置");
        }
        String actualToken = request.getHeader(TOKEN_HEADER);
        if (actualToken == null || !constantTimeEquals(expectedToken, actualToken)) {
            throw new AdminAuthorizationException(HttpStatus.UNAUTHORIZED, 40101, "清算后台鉴权失败");
        }
        String operatorId = request.getHeader(OPERATOR_HEADER);
        if (operatorId == null || operatorId.isBlank() || operatorId.length() > 64) {
            throw new AdminAuthorizationException(HttpStatus.BAD_REQUEST, 40001, "操作人标识缺失或非法");
        }
        return true;
    }

    /** 使用恒定时间比较，减少令牌逐字节推测风险。 */
    private boolean constantTimeEquals(String expected, String actual) {
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
    }
}
