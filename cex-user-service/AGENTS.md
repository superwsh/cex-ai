# `order-service/AGENTS.md`

## 1. 模块职责

Order Service 负责：

- 下单请求校验
- 订单创建
- 订单查询
- 订单状态管理
- 撤单请求
- 与资金冻结流程协调
- 向撮合系统发送订单 Command

Order Service 不负责：

- Order Book
- 撮合算法
- 成交价格计算
- 清算
- 行情计算

不得将 Matching Engine 业务逻辑复制到 Order Service。

---

## 2. 创建订单标准流程

创建订单时应检查：

```text
Request
↓
基础参数校验
↓
Symbol 校验
↓
价格/数量校验
↓
业务规则校验
↓
幂等检查
↓
资金冻结
↓
创建订单
↓
Outbox Event
↓
Commit
```

具体事务边界以当前项目架构为准。

---

## 3. 订单幂等

订单创建必须拥有稳定业务 ID：

```text
orderId
```

优先使用数据库唯一约束作为最终防线：

```sql
UNIQUE KEY uk_order_id(order_id)
```

重复 Order 请求不得：

- 创建两个订单
- 重复冻结资金
- 重复发送撮合命令

---

## 4. 订单状态机

状态变化必须通过领域模型或明确状态机。

示例：

```text
NEW
 ├─→ PARTIALLY_FILLED
 │       ├─→ FILLED
 │       └─→ CANCELLED
 │
 └─→ CANCELLED
```

实际状态以项目实现为准。

禁止非法流转，例如：

```text
FILLED → NEW
CANCELLED → PARTIALLY_FILLED
```

---

## 5. 撤单

实现撤单必须检查：

- Order 是否存在
- userId 是否匹配
- 当前状态是否允许撤销
- 是否已经撤销
- 是否已经全部成交
- 是否存在正在处理中的撮合事件
- 剩余 Quantity
- 剩余 Frozen Amount
- 是否需要通知 Matching Engine
- 是否需要释放资金

撤单不得简化为：

```java
order.setStatus(CANCELLED);
```

---

## 6. 订单与资金

任何订单金额计算必须明确：

```text
price
quantity
amount
fee
```

不得使用 `double`。

Order Service 不得直接通过随意 UPDATE 的方式修改账户余额。

账户变化应通过既有 Account 领域能力完成。

---

## 7. Outbox

如果订单创建与 Kafka Command 存在一致性要求：

```text
BEGIN

INSERT order

INSERT outbox_event

COMMIT
```

不得：

```text
INSERT order
COMMIT

producer.send()
```

然后假设消息永远不会丢失。

---

## 8. 订单事件

订单相关 Event / Command 必须明确：

- orderId
- userId
- symbol
- side
- orderType
- price
- quantity
- sequence/version
- eventId
- traceId

不得修改事件结构却不分析消费者兼容性。

---

## 9. 测试

至少根据修改范围覆盖：

```text
正常下单
重复 orderId
余额不足
非法 symbol
非法 price
非法 quantity
正常撤单
重复撤单
部分成交后撤单
全部成交后撤单
事务失败
Outbox 失败
```

涉及并发时增加并发测试。

