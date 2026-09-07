# `matching-engine/AGENTS.md`

## 1. 核心原则

Matching Engine 是整个交易系统最强调：

```text
确定性
顺序性
低延迟
可恢复性
```

的模块。

正确性和确定性优先于盲目提高线程数量。

---

## 2. 撮合原则

必须遵守：

```text
价格优先
时间优先
```

BUY：

```text
最高价格优先
```

SELL：

```text
最低价格优先
```

同一个价格：

```text
FIFO
```

任何修改不得破坏以上原则。

---

## 3. Single Writer

同一个 Order Book 必须遵循：

```text
Single Writer
```

即同一时间只允许一个执行上下文修改同一个 Symbol 的订单簿。

禁止为了性能随意引入：

- 多线程同时修改 TreeMap
- 多线程同时修改 PriceLevel
- 对同一 Symbol 并行撮合

---

## 4. Kafka Partition

同一个 Symbol 的指令必须维持全序。

推荐：

```text
symbol
↓
固定 Partition
↓
单 Consumer / Matching Worker
```

不得修改 Partition Key 而不分析顺序影响。

---

## 5. Order Book

逻辑结构：

```text
OrderBook
├── bids
│   └── Price → PriceLevel
└── asks
    └── Price → PriceLevel
```

典型实现：

```text
TreeMap<Price, PriceLevel>
```

PriceLevel 内：

```text
FIFO Orders
```

可维护：

```text
orderId → OrderReference
```

用于快速撤单。

---

## 6. PriceLevel

当一个 PriceLevel 中已经没有订单：

```text
PriceLevel.isEmpty()
```

应从 OrderBook 对应价格结构中移除。

不得留下大量空 PriceLevel。

---

## 7. 金额表示

不得使用 `double` 参与关键撮合价格和数量计算。

可以使用：

```text
BigDecimal
```

或者经架构明确批准的：

```text
long + scale
```

高性能 Fixed Point 模型。

---

## 8. Trade ID

每笔成交必须产生全局唯一、稳定的：

```text
tradeId
```

重复 Replay 相同输入时不得因为随机 ID 生成方式产生不同业务结果。

---

## 9. 确定性

相同：

```text
初始 Snapshot
+
完全相同的输入事件序列
```

必须得到完全相同：

```text
Order Book
Trade
Order State
Sequence
```

禁止核心撮合依赖：

```text
随机数
并发线程调度
无序容器遍历
当前系统时间作为逻辑排序依据
```

---

## 10. Sequence

进入 Matching Engine 的核心指令必须有明确顺序标识。

例如：

```text
sequence
```

成交输出也应可以关联到对应处理顺序。

---

## 11. WAL

关键输入应支持 WAL / Event Log。

至少考虑：

```text
PLACE_ORDER
CANCEL_ORDER
```

WAL 写入时机必须保证恢复逻辑不会漏掉已经进入订单簿的有效指令。

---

## 12. Snapshot

Snapshot 至少包含：

```text
Order Book State
lastSequence
```

恢复流程：

```text
读取最新 Snapshot
↓
恢复订单簿
↓
读取 lastSequence
↓
Replay sequence > lastSequence
↓
恢复到最新状态
```

修改 Order Book 数据结构时必须同步分析 Snapshot 序列化兼容性。

---

## 13. Replay

Replay 不能向外重复产生不可幂等的副作用。

需要明确区分：

```text
恢复内部状态
```

和：

```text
重新向 Kafka 发布业务事件
```

不得因为重放导致下游重复清算。

---

## 14. Matching 测试

至少覆盖：

```text
买单价格优先
卖单价格优先
相同价格时间优先
完全成交
部分成交
一对多成交
多对一成交
撤单
部分成交后撤单
PriceLevel 清理
空订单簿
Snapshot 恢复
WAL Replay
确定性 Replay
```



