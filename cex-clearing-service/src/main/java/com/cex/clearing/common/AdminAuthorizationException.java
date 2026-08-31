package com.cex.clearing.common;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/** 清算后台鉴权失败，携带明确 HTTP 状态但不暴露凭据详情。 */
@Getter
public class AdminAuthorizationException extends RuntimeException {
    private final HttpStatus httpStatus;
    private final int code;

    public AdminAuthorizationException(HttpStatus httpStatus, int code, String message) {
        super(message);
        this.httpStatus = httpStatus;
        this.code = code;
    }
}
