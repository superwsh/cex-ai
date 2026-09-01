package com.cex.market.interfaces.controller;

import com.cex.common.core.api.ApiResult;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.MissingServletRequestParameterException;

/** 行情 REST 参数错误的统一返回处理器。 */
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(basePackageClasses = MarketController.class)
public class MarketApiExceptionHandler {

    /**
     * 返回缺少必填参数的错误结果。
     *
     * @param exception Spring 参数绑定异常
     * @return 统一错误响应
     */
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ApiResult<Void> handleMissingParameter(MissingServletRequestParameterException exception) {
        return ApiResult.error(400, "缺少必填参数: " + exception.getParameterName());
    }

    /**
     * 返回参数类型不匹配的错误结果。
     *
     * @param exception Spring 参数绑定异常
     * @return 统一错误响应
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ApiResult<Void> handleTypeMismatch(MethodArgumentTypeMismatchException exception) {
        return ApiResult.error(400, "参数类型错误: " + exception.getName());
    }

    /**
     * 返回领域或应用层抛出的非法参数错误。
     *
     * @param exception 非法参数异常
     * @return 统一错误响应
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResult<Void> handleIllegalArgument(IllegalArgumentException exception) {
        return ApiResult.error(400, exception.getMessage());
    }
}
