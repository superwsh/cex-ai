-- Order Phase 6: 撮合撤单确认的累计成交数量。
USE cex_order;

ALTER TABLE orders
    ADD COLUMN cancel_confirmed_filled_quantity DECIMAL(32,16) NULL AFTER filled_amount;
