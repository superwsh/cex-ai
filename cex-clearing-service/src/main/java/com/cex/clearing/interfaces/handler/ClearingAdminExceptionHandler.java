package com.cex.clearing.interfaces.handler;

import com.cex.clearing.common.AdminAuthorizationException;
import com.cex.common.core.api.ApiResult;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** 将后台鉴权异常转换为正确 HTTP 状态和统一响应体。 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice
public class ClearingAdminExceptionHandler {

    /** 返回鉴权异常；响应中不包含配置令牌或请求令牌。 */
    @ExceptionHandler(AdminAuthorizationException.class)
    public ResponseEntity<ApiResult<Void>> handleAdminAuthorization(AdminAuthorizationException exception) {
        return ResponseEntity.status(exception.getHttpStatus())
                .body(ApiResult.error(exception.getCode(), exception.getMessage()));
    }
}
