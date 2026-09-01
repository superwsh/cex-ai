-- ============================================================
-- 行情订单簿恢复快照（Phase 8）
-- ============================================================

USE cex_market;

CREATE TABLE IF NOT EXISTS market_order_book_snapshot
(
    id                BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    symbol            VARCHAR(32)  NOT NULL COMMENT '交易对，如 BTC_USDT',
    snapshot_sequence BIGINT       NOT NULL COMMENT '快照包含的最后盘口序号',
    bids_json         JSON         NOT NULL COMMENT '买方全量价格档位 JSON，价格从高到低',
    asks_json         JSON         NOT NULL COMMENT '卖方全量价格档位 JSON，价格从低到高',
    kafka_partition   INT          NOT NULL COMMENT '对应 Kafka 分区',
    kafka_offset      BIGINT       NOT NULL COMMENT '对应 Kafka 位点',
    snapshot_time     BIGINT       NOT NULL COMMENT '快照创建时间（UTC 毫秒）',
    created_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_symbol (symbol),
    KEY idx_kafka_partition_offset (kafka_partition, kafka_offset)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '行情订单簿恢复快照';
