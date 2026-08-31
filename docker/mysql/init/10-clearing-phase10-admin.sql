-- Phase 10：清算后台人工操作审计。审计记录不得用于直接修改资金。
USE cex_account;

CREATE TABLE IF NOT EXISTS clearing_admin_operation_audit
(
    id             BIGINT       NOT NULL COMMENT '主键',
    operation_type VARCHAR(32)  NOT NULL COMMENT 'SETTLEMENT_RETRY/RECONCILIATION_RECHECK',
    target_id      VARCHAR(96)  NOT NULL COMMENT '操作目标',
    operator_id    VARCHAR(64)  NOT NULL COMMENT '操作人标识',
    request_id     VARCHAR(64)  NULL COMMENT '请求追踪ID',
    reason         VARCHAR(256) NOT NULL COMMENT '人工操作原因',
    before_status  VARCHAR(32)  NULL,
    after_status   VARCHAR(32)  NOT NULL,
    created_at     DATETIME     NOT NULL,
    PRIMARY KEY (id),
    KEY idx_admin_audit_target (operation_type, target_id, created_at),
    KEY idx_admin_audit_operator (operator_id, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '清算后台人工操作审计';
