-- ============================================================
-- Clearing Phase 3: SettlementTask 完整事件快照和重试字段
-- 新环境会在 03-clearing-tables.sql 后执行；已部署环境需在执行前确认列不存在。
-- ============================================================

USE cex_account;

ALTER TABLE settlement_task
    ADD COLUMN buy_order_id VARCHAR(64) NULL AFTER symbol,
    ADD COLUMN sell_order_id VARCHAR(64) NULL AFTER buy_order_id,
    ADD COLUMN buyer_user_id BIGINT NULL AFTER sell_order_id,
    ADD COLUMN seller_user_id BIGINT NULL AFTER buyer_user_id,
    ADD COLUMN base_asset VARCHAR(16) NULL AFTER seller_user_id,
    ADD COLUMN quote_asset VARCHAR(16) NULL AFTER base_asset,
    ADD COLUMN price DECIMAL(36,18) NULL AFTER quote_asset,
    ADD COLUMN quantity DECIMAL(36,18) NULL AFTER price,
    ADD COLUMN quote_amount DECIMAL(36,18) NULL AFTER quantity,
    ADD COLUMN buyer_fee DECIMAL(36,18) NOT NULL DEFAULT 0 AFTER quote_amount,
    ADD COLUMN buyer_fee_asset VARCHAR(16) NULL AFTER buyer_fee,
    ADD COLUMN seller_fee DECIMAL(36,18) NOT NULL DEFAULT 0 AFTER buyer_fee_asset,
    ADD COLUMN seller_fee_asset VARCHAR(16) NULL AFTER seller_fee,
    ADD COLUMN match_sequence BIGINT NULL AFTER seller_fee_asset,
    ADD COLUMN retry_count INT NOT NULL DEFAULT 0 AFTER status,
    ADD COLUMN next_retry_time DATETIME NULL AFTER retry_count,
    ADD COLUMN processing_at DATETIME NULL AFTER next_retry_time;

CREATE INDEX idx_settlement_status_retry ON settlement_task(status, next_retry_time);
CREATE INDEX idx_settlement_buyer_status ON settlement_task(buyer_user_id, status, created_at);
CREATE INDEX idx_settlement_seller_status ON settlement_task(seller_user_id, status, created_at);
