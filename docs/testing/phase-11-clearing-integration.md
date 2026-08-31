# Phase 11 清结算集成测试

## 自动化跨模块链路

`ClearingSettlementLifecycleIntegrationTest` 在同一 JVM 内串联订单、撮合和清算模块的真实领域及应用服务，持久化端口使用有状态 Mapper 替身。该测试适合每次提交和 CI 稳定执行，不依赖本机 Docker。

```powershell
mvn -pl cex-clearing-service -am "-Dtest=ClearingSettlementLifecycleIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false" test
```

自动验证链路：

```text
Create Order -> Freeze -> Matching -> TradeEvent(Kafka 消费边界)
-> Settlement -> Balance -> BalanceFlow/Journal -> Settlement Outbox
-> TradeSettledEvent(Kafka 消费边界) -> Order Update -> Order Unfreeze Outbox
-> Unfreeze -> Final Balance
```

覆盖断言：

- 买单按委托价冻结 10000 USDT，卖单冻结 0.1 BTC。
- 订单按更优挂单价 99000 成交，生成一笔 9900 USDT 的成交事件。
- 清算写入四笔余额过账、四笔 Journal 分录和一条结算 Outbox。
- 相同成交事件重复投递十次，余额、流水和 Outbox 均不重复变化。
- 订单更新事件重复投递不会重复累计成交。
- 买单成交价改善产生 100 USDT 解冻 Outbox；解冻事件重复十次只入账一次。
- 最终 USDT 和 BTC 总额守恒，且相关账户冻结余额全部归零。

## 真实 MySQL/Kafka 验收

组件集成测试不替代真实数据库事务、唯一索引、行锁以及 Kafka 重投递语义。发布前必须在可用 Docker 环境完成以下验收。

### 1. 启动并检查基础设施

```powershell
docker compose up -d mysql redis kafka nacos
docker compose ps
```

`mysql`、`redis`、`kafka` 应为 healthy，`nacos` 应处于 running。首次创建 MySQL 数据卷时，`docker/mysql/init` 下的 Phase 1–10 SQL 会按顺序执行。

### 2. 启动业务服务

分别启动清算、订单和撮合模块；三个进程必须指向同一 Kafka，订单与清算分别连接 `cex_order`、`cex_account`。

```powershell
mvn -pl cex-clearing-service -am spring-boot:run
mvn -pl cex-order-service -am spring-boot:run
mvn -pl cex-matching-engine -am spring-boot:run
```

### 3. 逐项验收

对一笔限价买单和一笔价格更优、数量相同的限价卖单执行完整成交，并记录 `order_id`、`trade_id`、`event_id`。随后检查：

```sql
USE cex_account;

SELECT user_id, asset, available, frozen
FROM account_balance
WHERE user_id IN (<buyer_user_id>, <seller_user_id>)
ORDER BY user_id, asset;

SELECT biz_type, biz_id, flow_type, user_id, asset,
       available_before, available_change, available_after,
       frozen_before, frozen_change, frozen_after
FROM balance_flow
WHERE biz_id IN (<buy_order_id>, <sell_order_id>, '<trade_id>')
ORDER BY created_at, id;

SELECT trade_id, status, retry_count, error_code
FROM settlement_task
WHERE trade_id = '<trade_id>';

SELECT journal_id, trade_id, status
FROM settlement_journal
WHERE trade_id = '<trade_id>';

SELECT journal_id, user_id, asset, account_type, amount, direction
FROM settlement_journal_entry
WHERE journal_id = CONCAT('trade-journal-', '<trade_id>')
ORDER BY id;

SELECT event_id, aggregate_id, topic, status, retry_count, last_error
FROM settlement_event_outbox
WHERE aggregate_id = '<trade_id>';
```

验收标准：

- `settlement_task.status = SUCCESS`，同一 `trade_id` 只有一条任务。
- 每个余额变化都能在 `balance_flow` 找到前后快照，账户余额不为负数。
- Journal 为 `SUCCESS`，分录数量与实际过账数量一致。
- 结算 Outbox 最终为 `PUBLISHED`；关闭 Kafka 时保留在 `RETRY`，恢复 Kafka 后可继续投递。
- 同一成交消息重复发送十次，余额与流水不增加。
- 在结算事务任一 SQL 处注入失败，账户、流水、Journal、任务成功状态和 Outbox 全部回滚。
- 数据库提交后、Kafka 投递前终止清算进程，重启后 Outbox 继续发送且订单只累计一次成交。
- 并发提交多笔涉及同一账户的成交，不出现负余额、死锁无限重试或资产不守恒。

## CI 分层建议

- 每次提交：运行全模块单元测试和本文件的跨模块组件测试。
- 合并前：使用独立 MySQL/Kafka 数据卷执行真实基础设施验收。
- 发布前：额外执行 Kafka 故障、进程终止、并发成交和恢复演练，并保存 SQL 快照及指标结果。
