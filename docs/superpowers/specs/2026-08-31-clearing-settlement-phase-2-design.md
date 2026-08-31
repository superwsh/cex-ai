# CEX 清结算系统 Phase 2 领域模型与 ER 设计

- 日期：2026-08-31
- 状态：Phase 2 已完成，等待进入 Phase 3
- 范围：领域模型、ER、目标 DDL、索引、幂等和事务边界设计；不在本阶段新增 Consumer、重试任务或对账任务实现。

## 1. 现有基础与设计决策

现有 `cex_account` 库已具备 `account_balance`、`account_operation`、`balance_flow`、`settlement_task` 和 `settlement_event_outbox` 的第一版建表及基础实现。本设计将其演进为完整的可审计模型，且不复制订单库的账户或订单表。

| 决策 | 选择 | 原因 |
|---|---|---|
| 账户事实来源 | `cex_account.account_balance` | 清算服务拥有余额写权限，订单服务仅经 Dubbo 冻结/解冻。 |
| 成交幂等 | `settlement_task.trade_id` 唯一 | Kafka 至少一次投递时的最终防线。 |
| 账务审计 | `balance_flow` + `journal` + `journal_entry` | 流水保留余额前后快照，Journal 表达一笔成交的完整复式分录。 |
| 事件一致性 | `settlement_event_outbox` | 余额、流水、结算状态与 `trade.settled` 同事务提交。 |
| 账户锁 | 涉及账户按 `account_balance.id ASC FOR UPDATE` | 同用户跨交易对并发时维持固定加锁顺序。 |
| 金额精度 | `DECIMAL(36,18)` / Java `BigDecimal` | 不使用 `double` 或未指定精度的除法。 |

`cex.trade.event` 仍为当前兼容 Topic；在 Phase 3 仅增加 Consumer，不修改 Topic 名称。后续统一命名时可引入 `cex.trade.executed`，但必须保留兼容消费者或完成全量切换。

## 2. 领域模型

```text
TradeEvent
  └─> SettlementTask（成交处理状态和重试信息）
         └─> ClearingResult（不可变计算结果）
                ├─> AccountPosting[]（账户余额变化）
                ├─> BalanceFlow[]（账户变更审计）
                ├─> Journal / JournalEntry[]（复式记账）
                └─> SettlementOutboxEvent（trade.settled）

ReconciliationResult
  └─> 指向 Trade / SettlementTask / BalanceFlow / Journal 的异常或差异
```

### 2.1 `SettlementTask`

状态机：

```text
INIT → PROCESSING → SUCCESS
                 ↘ RETRY → PROCESSING
                 ↘ FAILED / MANUAL_REVIEW
```

`trade_id` 是业务唯一键，`event_id` 是输入事件的唯一键。`PROCESSING` 不能被重复消息直接视为成功：若 `processing_at` 超时，由恢复任务转为 `RETRY`。

### 2.2 `ClearingResult` 与 `AccountPosting`

`ClearingCalculator` 只计算，不访问数据库。输入为已冻结费率的 `TradeEvent`，输出四类标准过账：

| 参与方 | 资产 | 冻结变化 | 可用变化 |
|---|---|---:|---:|
| 买方 | quote | `-quoteAmount` | `0` |
| 买方 | base | `0` | `quantity - buyerFee(base)` |
| 卖方 | base | `-quantity` | `0` |
| 卖方 | quote | `0` | `quoteAmount - sellerFee(quote)` |
| 平台 | feeAsset | `0` | `+fee` |

事件中的费用是历史成交事实；`FeeCalculator` 仅在撮合前或生成 TradeEvent 时决定费率，不允许结算时按当前 VIP 等级重算。

### 2.3 `BalanceFlow`

每一条资金变动产生一条 append-only `BalanceFlow`，保留 `available/frozen` 变更前、变化量和变更后。`TRADE_BUY_QUOTE`、`TRADE_BUY_BASE`、`TRADE_SELL_BASE`、`TRADE_SELL_QUOTE`、`TRADE_FEE` 是同一 trade 下不同合法流水，不能以 `(trade_id,user_id,asset)` 作为唯一键。

### 2.4 `Journal` / `JournalEntry`

一个 Trade 对应一条 `Journal`，分录使用内部账户：`USER_AVAILABLE`、`USER_FROZEN`、`PLATFORM_FEE`。每个 `(journal_id, asset)` 的借贷金额必须相等；此校验由 `JournalService` 在插入前完成。

## 3. ER 图

```text
account_balance 1 ──── * balance_flow
       │                         │
       │                         └─── * journal_entry * ──── 1 journal
       │
       └──── * account_operation

settlement_task (trade_id UNIQUE)
       ├──── 1 journal (biz_id = trade_id)
       ├──── * balance_flow (biz_type=TRADE_SETTLEMENT, biz_id=trade_id)
       ├──── * settlement_event_outbox
       └──── * reconciliation_result
```

`account_balance` 是当前余额快照，不用于替代流水。`balance_flow` 和 Journal 历史记录禁止 UPDATE/DELETE；修正只能以 `COMPENSATION` 反向流水完成。

## 4. 目标 DDL（Phase 3 前执行的演进目标）

现有 [03-clearing-tables.sql](../../../docker/mysql/init/03-clearing-tables.sql) 是首次启动初始化脚本。已部署环境应使用版本化 migration 完成下列字段补齐，而不是修改历史生产记录。

```sql
ALTER TABLE settlement_task
    ADD COLUMN buy_order_id VARCHAR(64) NOT NULL AFTER symbol,
    ADD COLUMN sell_order_id VARCHAR(64) NOT NULL AFTER buy_order_id,
    ADD COLUMN buyer_user_id BIGINT NOT NULL AFTER sell_order_id,
    ADD COLUMN seller_user_id BIGINT NOT NULL AFTER buyer_user_id,
    ADD COLUMN base_asset VARCHAR(16) NOT NULL AFTER seller_user_id,
    ADD COLUMN quote_asset VARCHAR(16) NOT NULL AFTER base_asset,
    ADD COLUMN price DECIMAL(36,18) NOT NULL AFTER quote_asset,
    ADD COLUMN quantity DECIMAL(36,18) NOT NULL AFTER price,
    ADD COLUMN quote_amount DECIMAL(36,18) NOT NULL AFTER quantity,
    ADD COLUMN buyer_fee DECIMAL(36,18) NOT NULL DEFAULT 0 AFTER quote_amount,
    ADD COLUMN buyer_fee_asset VARCHAR(16) NULL AFTER buyer_fee,
    ADD COLUMN seller_fee DECIMAL(36,18) NOT NULL DEFAULT 0 AFTER buyer_fee_asset,
    ADD COLUMN seller_fee_asset VARCHAR(16) NULL AFTER seller_fee,
    ADD COLUMN match_sequence BIGINT NOT NULL AFTER seller_fee_asset,
    ADD COLUMN retry_count INT NOT NULL DEFAULT 0 AFTER status,
    ADD COLUMN next_retry_time DATETIME NULL AFTER retry_count,
    ADD COLUMN processing_at DATETIME NULL AFTER next_retry_time;

CREATE INDEX idx_settlement_status_retry
    ON settlement_task(status, next_retry_time);
CREATE INDEX idx_settlement_buyer_status
    ON settlement_task(buyer_user_id, status, created_at);
CREATE INDEX idx_settlement_seller_status
    ON settlement_task(seller_user_id, status, created_at);

CREATE TABLE journal (
    id BIGINT NOT NULL,
    journal_id VARCHAR(64) NOT NULL,
    biz_type VARCHAR(32) NOT NULL,
    biz_id VARCHAR(64) NOT NULL,
    trade_id VARCHAR(64) NULL,
    status VARCHAR(16) NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_journal_id (journal_id),
    UNIQUE KEY uk_biz (biz_type, biz_id),
    KEY idx_trade_id (trade_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='不可变账务凭证';

CREATE TABLE journal_entry (
    id BIGINT NOT NULL,
    journal_id VARCHAR(64) NOT NULL,
    user_id BIGINT NULL,
    asset VARCHAR(16) NOT NULL,
    account_code VARCHAR(32) NOT NULL,
    direction VARCHAR(6) NOT NULL,
    amount DECIMAL(36,18) NOT NULL,
    created_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_journal_account_direction (journal_id, user_id, asset, account_code, direction),
    KEY idx_journal_id (journal_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账务凭证分录';

CREATE TABLE reconciliation_result (
    id BIGINT NOT NULL,
    reconciliation_type VARCHAR(32) NOT NULL,
    biz_id VARCHAR(64) NULL,
    trade_id VARCHAR(64) NULL,
    user_id BIGINT NULL,
    asset VARCHAR(16) NULL,
    expected_amount DECIMAL(36,18) NULL,
    actual_amount DECIMAL(36,18) NULL,
    difference_amount DECIMAL(36,18) NULL,
    status VARCHAR(16) NOT NULL,
    error_message VARCHAR(1024) NULL,
    created_at DATETIME NOT NULL,
    resolved_at DATETIME NULL,
    PRIMARY KEY (id),
    KEY idx_reconciliation_status_created (status, created_at),
    KEY idx_reconciliation_trade (trade_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='清结算对账差异';
```

## 5. 唯一索引与幂等策略

| 写操作 | 幂等键 | 数据库约束 | 重复处理结果 |
|---|---|---|---|
| 成交结算 | `tradeId` | `uk_trade_id` | `SUCCESS` 直接 ACK；非成功状态按恢复策略处理。 |
| 冻结/解冻 | `bizType + bizId + operationType + userId + asset` | 账户命令唯一键 | 不重复修改余额。 |
| 资金流水 | `bizType + bizId + flowType + userId + asset` | 流水唯一键 | 避免重放插入重复流水。 |
| Journal | `bizType + bizId` | `uk_biz` | 一笔 Trade 仅一张凭证。 |
| Outbox | `eventId` | `uk_event_id` | 允许发送重试，不允许生成第二条业务事件。 |

## 6. 事务边界与锁顺序

单笔结算采用一个本地事务：

```text
创建/锁定 SettlementTask
→ 校验 TradeEvent 与固定费用
→ 按 account_balance.id ASC SELECT ... FOR UPDATE
→ 原子扣减冻结 / 增加可用
→ INSERT balance_flow
→ INSERT journal + journal_entry
→ UPDATE settlement_task SUCCESS
→ INSERT settlement_event_outbox
→ COMMIT
```

任何账户更新受 `available >= amount` 或 `frozen >= amount` 的 SQL 条件保护，并检查 affected rows。事务中不得发 Kafka、调用远程 RPC 或执行长时间操作。Kafka offset 仅在事务成功返回后由容器提交。

## 7. 与当前实现的差异和 Phase 3 输入

当前基础实现已经有账户原子更新、流水、任务和 Outbox，但尚未持久化完整 Trade 快照、Journal/JournalEntry、任务重试字段和对账结果。Phase 3 必须先按本设计补齐 `settlement_task` 字段及 Journal DDL，再实现 Consumer 的参数校验、异常分类、Retry/DLQ。

当前的零手续费保护应保留：在 `FeeCalculator` 和平台手续费账户同时落地前，拒绝非零费用事件，不能静默忽略手续费。
