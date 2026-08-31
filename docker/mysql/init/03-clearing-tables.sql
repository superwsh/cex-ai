-- ============================================================
-- CEX 清算结算库表结构（库：cex_account）
-- 账户原子变更、幂等命令和资金流水必须在同一事务内提交。
-- ============================================================

USE cex_account;

CREATE TABLE IF NOT EXISTS account_balance
(
    id         BIGINT         NOT NULL COMMENT '主键',
    user_id    BIGINT         NOT NULL COMMENT '用户ID',
    asset      VARCHAR(16)    NOT NULL COMMENT '资产代码',
    available  DECIMAL(36,18) NOT NULL DEFAULT 0 COMMENT '可用余额',
    frozen     DECIMAL(36,18) NOT NULL DEFAULT 0 COMMENT '冻结余额',
    version    BIGINT         NOT NULL DEFAULT 0 COMMENT '版本号',
    created_at DATETIME       NOT NULL,
    updated_at DATETIME       NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_user_asset (user_id, asset),
    CONSTRAINT chk_account_available_nonnegative CHECK (available >= 0),
    CONSTRAINT chk_account_frozen_nonnegative CHECK (frozen >= 0)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '用户资产余额';

CREATE TABLE IF NOT EXISTS account_operation
(
    id             BIGINT      NOT NULL COMMENT '主键',
    biz_type       VARCHAR(32) NOT NULL COMMENT '业务类型',
    biz_id         VARCHAR(64) NOT NULL COMMENT '业务ID',
    operation_type VARCHAR(16) NOT NULL COMMENT 'FREEZE/UNFREEZE',
    user_id        BIGINT      NOT NULL COMMENT '用户ID',
    asset          VARCHAR(16) NOT NULL COMMENT '资产代码',
    created_at     DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_biz_operation (biz_type, biz_id, operation_type)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '账户命令幂等记录';

CREATE TABLE IF NOT EXISTS balance_flow
(
    id               BIGINT         NOT NULL COMMENT '主键',
    biz_type         VARCHAR(32)    NOT NULL COMMENT '业务类型',
    biz_id           VARCHAR(64)    NOT NULL COMMENT '业务ID',
    flow_type        VARCHAR(32)    NOT NULL COMMENT 'ORDER_FREEZE/ORDER_UNFREEZE 等',
    user_id          BIGINT         NOT NULL COMMENT '用户ID',
    asset            VARCHAR(16)    NOT NULL COMMENT '资产代码',
    available_before DECIMAL(36,18) NOT NULL,
    available_change DECIMAL(36,18) NOT NULL,
    available_after  DECIMAL(36,18) NOT NULL,
    frozen_before    DECIMAL(36,18) NOT NULL,
    frozen_change    DECIMAL(36,18) NOT NULL,
    frozen_after     DECIMAL(36,18) NOT NULL,
    created_at       DATETIME       NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_biz_flow_account (biz_type, biz_id, flow_type, user_id, asset),
    KEY idx_user_asset_created (user_id, asset, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '不可变余额流水';

CREATE TABLE IF NOT EXISTS settlement_task
(
    id             BIGINT      NOT NULL COMMENT '主键',
    trade_id       VARCHAR(64) NOT NULL COMMENT '成交ID',
    event_id       VARCHAR(64) NOT NULL COMMENT '输入事件ID',
    symbol         VARCHAR(32) NOT NULL,
    status         VARCHAR(16) NOT NULL COMMENT 'PROCESSING/SUCCESS',
    error_code     VARCHAR(64) NULL COMMENT '最近一次结算错误码',
    error_message  VARCHAR(512) NULL,
    created_at     DATETIME    NOT NULL,
    settled_at     DATETIME    NULL,
    updated_at     DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_trade_id (trade_id),
    UNIQUE KEY uk_event_id (event_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '成交结算幂等任务';

CREATE TABLE IF NOT EXISTS settlement_event_outbox
(
    id              BIGINT      NOT NULL COMMENT '主键',
    event_id        VARCHAR(64) NOT NULL,
    aggregate_id    VARCHAR(64) NOT NULL COMMENT 'tradeId',
    topic           VARCHAR(128) NOT NULL,
    payload         TEXT        NOT NULL,
    status          VARCHAR(16) NOT NULL DEFAULT 'NEW' COMMENT 'NEW/SENDING/RETRY/PUBLISHED/FAILED',
    retry_count     INT         NOT NULL DEFAULT 0,
    next_retry_time DATETIME    NOT NULL,
    processing_token VARCHAR(64) NULL COMMENT '发布租约令牌',
    last_error      VARCHAR(512) NULL COMMENT '最近一次投递错误摘要',
    created_at      DATETIME    NOT NULL,
    published_at    DATETIME    NULL,
    updated_at      DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_status_retry (status, next_retry_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '结算事件发件箱';
