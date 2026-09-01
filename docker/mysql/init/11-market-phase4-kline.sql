-- ============================================================
-- 行情库与 KLine 表（Phase 4）
-- ============================================================

CREATE DATABASE IF NOT EXISTS cex_market DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE cex_market;

CREATE TABLE IF NOT EXISTS market_kline
(
    id           BIGINT          NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    symbol       VARCHAR(32)     NOT NULL COMMENT '交易对，如 BTC_USDT',
    `interval`   VARCHAR(8)      NOT NULL COMMENT 'KLine 周期，如 1m',
    open_time    BIGINT          NOT NULL COMMENT 'UTC Epoch 窗口开始时间（毫秒）',
    close_time   BIGINT          NOT NULL COMMENT 'UTC Epoch 窗口结束时间（毫秒）',
    open_price   DECIMAL(32,16)  NOT NULL COMMENT '开盘价',
    high_price   DECIMAL(32,16)  NOT NULL COMMENT '最高价',
    low_price    DECIMAL(32,16)  NOT NULL COMMENT '最低价',
    close_price  DECIMAL(32,16)  NOT NULL COMMENT '收盘价',
    volume       DECIMAL(32,16)  NOT NULL COMMENT '基础资产成交量',
    quote_volume DECIMAL(32,16)  NOT NULL COMMENT '计价资产成交量',
    trade_count  BIGINT          NOT NULL COMMENT '成交笔数',
    created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_symbol_interval_open_time (symbol, `interval`, open_time),
    KEY idx_symbol_interval_open_time (symbol, `interval`, open_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '已收线 KLine 历史数据';
