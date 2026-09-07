# `market-service/AGENTS.md`

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