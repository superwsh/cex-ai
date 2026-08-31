-- Phase 7：为既有结算 Outbox 增加跨实例原子认领和可审计失败信息。
USE cex_account;

ALTER TABLE settlement_event_outbox
    ADD COLUMN IF NOT EXISTS processing_token VARCHAR(64) NULL COMMENT '发布租约令牌' AFTER next_retry_time,
    ADD COLUMN IF NOT EXISTS last_error VARCHAR(512) NULL COMMENT '最近一次投递错误摘要' AFTER processing_token;
