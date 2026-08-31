-- ============================================================
-- CEX 订单库表结构(Phase 1)
-- 库:cex_order
-- 注意:order_event_outbox / processed_event 为事务性发件箱与消费幂等表
-- ============================================================

USE cex_order;

-- 订单表
CREATE TABLE IF NOT EXISTS orders
(
    id               BIGINT        NOT NULL COMMENT '主键(自研 Snowflake)',
    order_id         BIGINT        NOT NULL COMMENT '订单ID(业务唯一,与主键同值)',
    user_id          BIGINT        NOT NULL COMMENT '用户ID',
    client_order_id  VARCHAR(64)   NULL COMMENT '客户端订单号(幂等键)',
    symbol           VARCHAR(32)   NOT NULL COMMENT '交易对,如 BTC_USDT',
    side             VARCHAR(10)   NOT NULL COMMENT 'BUY/SELL',
    type             VARCHAR(20)   NOT NULL COMMENT 'LIMIT/MARKET',
    price            DECIMAL(32,16) NULL COMMENT '委托价格(市价单为NULL)',
    quantity         DECIMAL(32,16) NOT NULL COMMENT '委托数量',
    quote_amount     DECIMAL(32,16) NULL COMMENT '市价买单冻结金额',
    filled_quantity  DECIMAL(32,16) NOT NULL DEFAULT 0 COMMENT '已成交数量',
    filled_amount    DECIMAL(32,16) NOT NULL DEFAULT 0 COMMENT '已成交金额',
    status           VARCHAR(32)   NOT NULL COMMENT 'NEW/PENDING_MATCH/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED',
    time_in_force    VARCHAR(10)   NULL COMMENT 'GTC/IOC/FOK',
    version          BIGINT        NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
    created_at       DATETIME      NOT NULL COMMENT '创建时间',
    updated_at       DATETIME      NOT NULL COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_id (order_id),
    UNIQUE KEY uk_user_client_order (user_id, client_order_id),
    KEY idx_user_symbol_time (user_id, symbol, created_at),
    KEY idx_user_status_time (user_id, status, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '订单表';

-- 订单事件发件箱(Transactional Outbox)
CREATE TABLE IF NOT EXISTS order_event_outbox
(
    id              BIGINT      NOT NULL COMMENT '主键',
    event_id        VARCHAR(64) NOT NULL COMMENT '事件ID(UUID)',
    aggregate_type  VARCHAR(32) NOT NULL COMMENT '聚合类型,如 ORDER',
    aggregate_id    VARCHAR(64) NOT NULL COMMENT '聚合ID,如 orderId',
    event_type      VARCHAR(64) NOT NULL COMMENT 'ORDER_CREATED/ORDER_CANCELED',
    payload         TEXT        NOT NULL COMMENT '事件JSON',
    status          VARCHAR(16) NOT NULL DEFAULT 'INIT' COMMENT 'INIT/SENDING/SUCCESS/FAILED',
    retry_count     INT         NOT NULL DEFAULT 0 COMMENT '已重试次数',
    next_retry_time DATETIME    NOT NULL COMMENT '下次重试时间(指数退避)',
    created_at      DATETIME    NOT NULL,
    updated_at      DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_status_next_retry (status, next_retry_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '订单事件发件箱';

-- 撮合命令交易对内序号：与订单和 Outbox 事件在同一事务中更新，作为跨实例恢复的唯一顺序来源
CREATE TABLE IF NOT EXISTS matching_command_sequence
(
    symbol        VARCHAR(32) NOT NULL COMMENT '交易对,如 BTC_USDT',
    last_sequence BIGINT      NOT NULL COMMENT '已分配的最大撮合命令序号',
    updated_at    DATETIME    NOT NULL COMMENT '最后分配时间',
    PRIMARY KEY (symbol)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '撮合命令交易对内序号';

-- 已处理事件表(消费幂等)
CREATE TABLE IF NOT EXISTS processed_event
(
    id           BIGINT      NOT NULL COMMENT '主键',
    event_id     VARCHAR(64) NOT NULL COMMENT '事件ID(如 tradeId)',
    consumer     VARCHAR(64) NOT NULL COMMENT '消费者标识,如 ORDER_STATUS_CONSUMER',
    processed_at DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_consumer (event_id, consumer)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '已处理事件(幂等)';

-- 交易对配置表
CREATE TABLE IF NOT EXISTS symbol_config
(
    id             BIGINT         NOT NULL COMMENT '主键',
    symbol         VARCHAR(32)    NOT NULL COMMENT '交易对,如 BTC_USDT',
    base_currency  VARCHAR(16)    NOT NULL COMMENT '基础币,如 BTC',
    quote_currency VARCHAR(16)    NOT NULL COMMENT '计价币,如 USDT',
    price_scale    INT            NOT NULL COMMENT '价格精度(小数位数)',
    quantity_scale INT            NOT NULL COMMENT '数量精度(小数位数)',
    min_quantity   DECIMAL(32,16) NOT NULL COMMENT '最小下单数量',
    min_amount     DECIMAL(32,16) NOT NULL COMMENT '最小下单金额',
    status         VARCHAR(16)    NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE/PAUSED',
    created_at     DATETIME       NOT NULL,
    updated_at     DATETIME       NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_symbol (symbol)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '交易对配置';

-- 预置交易对数据
INSERT INTO symbol_config
    (id, symbol, base_currency, quote_currency, price_scale, quantity_scale, min_quantity, min_amount, status, created_at, updated_at)
VALUES
    (1, 'BTC_USDT', 'BTC', 'USDT', 2, 6, 0.00010000000000000000, 10.00000000000000000000, 'ACTIVE', NOW(), NOW()),
    (2, 'ETH_USDT', 'ETH', 'USDT', 2, 4, 0.01000000000000000000, 10.00000000000000000000, 'ACTIVE', NOW(), NOW());
