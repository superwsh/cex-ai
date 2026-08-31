package com.cex.clearing.domain.settlement;

/**
 * 结算异常，明确表达是否允许 Kafka 自动重试。
 */
public class SettlementException extends RuntimeException {

    private final String errorCode;
    private final boolean retryable;

    public SettlementException(String errorCode, String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    public SettlementException(String errorCode, String message, boolean retryable) {
        this(errorCode, message, retryable, null);
    }

    public String getErrorCode() {
        return errorCode;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
