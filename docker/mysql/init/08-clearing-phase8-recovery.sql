-- Phase 8：持久化成交时间，支持无需依赖 Kafka 原消息的结算恢复。
USE cex_account;

ALTER TABLE settlement_task
    ADD COLUMN IF NOT EXISTS error_code VARCHAR(64) NULL COMMENT '最近一次结算错误码' AFTER status,
    ADD COLUMN IF NOT EXISTS trade_time BIGINT NULL COMMENT '撮合成交时间戳（毫秒）' AFTER match_sequence;
