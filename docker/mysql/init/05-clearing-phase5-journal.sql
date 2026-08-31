-- ============================================================
-- Clearing Phase 5: 结算凭证（Journal）和失败错误码
-- 新环境会在 03、04 后执行；已部署环境需确认对象或字段不存在。
-- ============================================================

USE cex_account;

ALTER TABLE settlement_task
    ADD COLUMN IF NOT EXISTS error_code VARCHAR(64) NULL AFTER error_message;

CREATE TABLE IF NOT EXISTS settlement_journal
(
    id         BIGINT      NOT NULL COMMENT '主键',
    journal_id VARCHAR(96) NOT NULL COMMENT '业务凭证ID',
    biz_type   VARCHAR(32) NOT NULL COMMENT '业务类型',
    biz_id     VARCHAR(64) NOT NULL COMMENT '业务ID',
    trade_id   VARCHAR(64) NOT NULL COMMENT '成交ID',
    status     VARCHAR(16) NOT NULL COMMENT 'PROCESSING/SUCCESS',
    created_at DATETIME    NOT NULL,
    updated_at DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_journal_id (journal_id),
    UNIQUE KEY uk_journal_trade (trade_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '成交结算复式记账凭证';

CREATE TABLE IF NOT EXISTS settlement_journal_entry
(
    id           BIGINT         NOT NULL COMMENT '主键',
    journal_id   VARCHAR(96)    NOT NULL COMMENT '业务凭证ID',
    user_id      BIGINT         NOT NULL COMMENT '用户或平台账户ID',
    asset        VARCHAR(16)    NOT NULL COMMENT '资产代码',
    account_type VARCHAR(32)    NOT NULL COMMENT 'USER_AVAILABLE/USER_FROZEN/PLATFORM_FEE',
    amount       DECIMAL(36,18) NOT NULL COMMENT '绝对金额',
    direction    VARCHAR(8)     NOT NULL COMMENT 'DEBIT/CREDIT',
    created_at   DATETIME       NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_journal_account (journal_id, user_id, asset, account_type, direction),
    KEY idx_journal_id (journal_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '成交结算复式记账分录';
