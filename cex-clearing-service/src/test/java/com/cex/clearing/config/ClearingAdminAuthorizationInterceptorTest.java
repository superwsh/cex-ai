package com.cex.clearing.config;

import com.cex.clearing.common.AdminAuthorizationException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 清算后台默认拒绝鉴权测试。 */
class ClearingAdminAuthorizationInterceptorTest {

    /** 令牌和操作人均有效时允许访问。 */
    @Test
    void shouldAllowRequestWithValidTokenAndOperator() {
        ClearingAdminProperties properties = new ClearingAdminProperties();
        properties.setToken("secret-token");
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(ClearingAdminAuthorizationInterceptor.TOKEN_HEADER)).thenReturn("secret-token");
        when(request.getHeader(ClearingAdminAuthorizationInterceptor.OPERATOR_HEADER)).thenReturn("operator-1");
        ClearingAdminAuthorizationInterceptor interceptor = new ClearingAdminAuthorizationInterceptor(properties);

        boolean allowed = interceptor.preHandle(request, mock(HttpServletResponse.class), new Object());

        assertThat(allowed).isTrue();
    }

    /** 令牌错误时必须返回未授权语义。 */
    @Test
    void shouldRejectRequestWithInvalidToken() {
        ClearingAdminProperties properties = new ClearingAdminProperties();
        properties.setToken("secret-token");
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getHeader(ClearingAdminAuthorizationInterceptor.TOKEN_HEADER)).thenReturn("wrong-token");
        ClearingAdminAuthorizationInterceptor interceptor = new ClearingAdminAuthorizationInterceptor(properties);

        assertThatThrownBy(() -> interceptor.preHandle(request, mock(HttpServletResponse.class), new Object()))
                .isInstanceOfSatisfying(AdminAuthorizationException.class,
                        exception -> assertThat(exception.getHttpStatus().value()).isEqualTo(401));
    }

    /** 服务未配置后台令牌时必须默认拒绝全部访问。 */
    @Test
    void shouldFailClosedWhenAdminTokenIsNotConfigured() {
        ClearingAdminProperties properties = new ClearingAdminProperties();
        ClearingAdminAuthorizationInterceptor interceptor = new ClearingAdminAuthorizationInterceptor(properties);

        assertThatThrownBy(() -> interceptor.preHandle(mock(HttpServletRequest.class),
                mock(HttpServletResponse.class), new Object()))
                .isInstanceOfSatisfying(AdminAuthorizationException.class,
                        exception -> assertThat(exception.getHttpStatus().value()).isEqualTo(503));
    }
}
