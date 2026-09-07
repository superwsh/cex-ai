# `clearing-service/AGENTS.md`

## 1. 模块性质

Clearing Service 是核心资金模块。

禁止在未充分分析的情况下修改清算逻辑。

重点保证：

```text
一次成交
=
一次且仅一次经济效果
```

---

## 2. 输入

主要输入来源：

```text
Trade Event
```

必须以：

```text
tradeId
```

为核心业务标识之一。

---

## 3. 买方清算

以 BTC/USDT 为例。

买方典型变化：

```text
USDT Frozen ↓
BTC Available ↑
```

并根据规则扣除：

```text
Trading Fee
```

---

## 4. 卖方清算

卖方典型变化：

```text
BTC Frozen ↓
USDT Available ↑
```

并根据规则扣除手续费。

---

## 5. 幂等

重复收到同一个 Trade Event：

```text
tradeId = X
```

不得重复：

- Buyer 入账
- Seller 入账
- Frozen 扣减
- Fee 扣减
- Fund Flow

数据库必须拥有最终幂等保证。

---

## 6. 资金流水

推荐根据业务使用类似：

```text
tradeId + userId + bizType
```

建立唯一约束。

例如：

```text
TRADE_BUY
TRADE_SELL
TRADE_FEE
```

具体枚举以当前系统设计为准。

---

## 7. 本地事务

如果相关账户和流水位于同一数据库，可考虑：

```text
BEGIN

买方资产变化
卖方资产变化
手续费账户变化

Buyer Fund Flow
Seller Fund Flow
Fee Fund Flow

Settlement Status

COMMIT
```

如果数据库已经分片，不得盲目照搬单库大事务。

---

## 8. Offset

Kafka Offset 的处理必须保证：

```text
业务事务成功后
↓
才能认为消息成功处理
```

不能：

```text
先提交 Offset
↓
再执行资金更新
```

否则故障时可能永久丢失清算。

---

## 9. 消费失败

Consumer 必须区分：

```text
可重试异常
不可重试业务异常
数据异常
```

禁止无限 Retry 永久阻塞 Partition。

应结合项目设计使用：

- retry
- retry topic
- dead letter
- alarm
- reconciliation

---

## 10. 对账

清算数据必须能够与：

```text
Trade
Fund Flow
Account
Order
```

进行对账。

发现不一致不得无记录地直接修余额。

---

## 11. Clearing 测试

至少覆盖：

```text
正常清算
重复 Trade
买方变化
卖方变化
手续费
余额不足异常
事务回滚
Consumer 重复消费
Consumer 重启
处理成功但 Offset 未提交
数据库超时
```

---

# `market-data/AGENTS.md`

## 1. 模块定位

Market Data 是派生数据系统。

它不得反向决定：

- Account Balance
- Order 最终状态
- Trade 成交事实
- Clearing 最终状态

核心事实来源应来自：

```text
Matching Engine / Trade Event
```

---

## 2. 主要能力

Market Data 包括：

```text
Trade
Ticker
Depth
Kline
24h Statistics
Order Book Snapshot
WebSocket Push
```

---

## 3. Sequence

Depth 和增量 Order Book 数据必须有顺序标识。

例如：

```text
sequence
```

客户端不得依赖消息到达时间猜测顺序。

---

## 4. Depth Snapshot

典型客户端同步模式：

```text
请求 Snapshot
↓
取得 lastSequence
↓
消费增量事件
↓
应用 sequence > lastSequence 的事件
```

---

## 5. Sequence Gap

如果客户端或内部服务发现：

```text
expected = 1002
actual = 1005
```

说明出现 Gap。

不得继续盲目应用后续增量。

应该：

```text
停止应用
↓
重新获取 Snapshot
↓
重新建立增量链路
```

---

## 6. Trade Data

成交行情必须来源于真实 Trade Event。

不得通过：

```text
Order 状态变化
```

反推成交记录。

---

## 7. Kline

Kline 聚合必须明确：

- interval
- openTime
- closeTime
- open
- high
- low
- close
- volume
- quoteVolume
- tradeCount

必须明确迟到事件和重复事件的处理规则。

---

## 8. WebSocket

WebSocket 推送属于：

```text
Best Effort Delivery
```

客户端必须能够通过 REST Snapshot / Sequence 恢复状态。

不要把 WebSocket 推送成功作为核心业务成功条件。

---

## 9. Backpressure

面对慢客户端：

不得让单个客户端阻塞整个行情生产链路。

需要考虑：

- bounded queue
- disconnect slow client
- batching
- event coalescing

具体实现以现有架构为准。

---

## 10. Market Data 测试

至少根据修改范围覆盖：

```text
Trade 推送
Depth 顺序
Sequence Gap
Snapshot 恢复
Ticker
Kline
重复 Trade
消息乱序
慢客户端
断线重连
```