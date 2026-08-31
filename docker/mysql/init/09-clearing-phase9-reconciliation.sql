-- Phase 9：只读对账问题记录。补偿或人工处理必须另行产生流水和 Journal。
USE cex_account;

CREATE TABLE IF NOT EXISTS reconciliation_result
(
    id                  BIGINT         NOT NULL COMMENT '主键',
    reconciliation_type VARCHAR(32)    NOT NULL COMMENT 'TRADE_SETTLEMENT/SETTLEMENT_LEDGER/ACCOUNT_LEDGER',
    biz_id              VARCHAR(96)    NOT NULL COMMENT '成交ID或账户标识',
    trade_id            VARCHAR(64)    NULL COMMENT '成交ID',
    user_id             BIGINT         NULL COMMENT '账户用户ID',
    asset               VARCHAR(16)    NULL COMMENT '资产代码',
    check_item          VARCHAR(32)    NOT NULL COMMENT 'SETTLEMENT_SUCCESS/JOURNAL_AND_FLOW/AVAILABLE/FROZEN',
    expected_amount     DECIMAL(36,18) NOT NULL,
    actual_amount       DECIMAL(36,18) NOT NULL,
    difference          DECIMAL(36,18) NOT NULL COMMENT 'actual - expected',
    status              VARCHAR(16)    NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN/CONFIRMED/COMPENSATED/IGNORED',
    error_message       VARCHAR(512)   NOT NULL,
    created_at          DATETIME       NOT NULL,
    updated_at          DATETIME       NOT NULL,
    resolved_at         DATETIME       NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_reconciliation_issue (reconciliation_type, biz_id, check_item),
    KEY idx_reconciliation_status_created (status, created_at),
    KEY idx_reconciliation_trade (trade_id),
    KEY idx_reconciliation_account (user_id, asset, status)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '清算对账差异结果';
