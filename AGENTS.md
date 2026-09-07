# CEX 全局 Coding Agent 规则

## 1. 项目性质

本项目是中心化交易所 CEX 后端系统。

这是金融交易系统。

任何代码修改都必须优先保证：

1. 正确性
2. 资金安全
3. 数据一致性
4. 幂等性
5. 顺序性
6. 确定性
7. 可恢复性
8. 可审计性
9. 性能
10. 代码可维护性

不得为了快速完成任务而降低资金安全、一致性或可恢复性。

---

## 2. 系统模块

当前核心模块：

- `order-service`
- `account-service`
- `matching-engine`
- `clearing-service`
- `market-data`

后续可能包含：

- `gateway`
- `reconciliation-service`
- `risk-service`
- `wallet-service`

必须严格遵守模块边界。

除非任务明确要求，不允许：

- 将业务职责随意移动到其他模块
- 新增不必要的跨服务调用
- 将异步架构随意改成同步架构
- 将领域逻辑放入 Controller
- 将核心业务逻辑放入 Kafka Consumer
- 为了省事直接绕过既有领域模型

---

## 3. 核心业务链路

典型订单链路：

```text
Client
  ↓
Gateway
  ↓
Order Service
  ↓
Account Service / Freeze
  ↓
Outbox
  ↓
Kafka
  ↓
Matching Engine
  ↓
Trade Event
  ↓
Kafka
  ↓
Clearing Service
  ↓
Account Settlement
  ↓
Market Data
```

修改任意核心链路前，必须分析上下游影响。

---

## 4. Agent 开发流程

收到开发任务后，不要立即修改代码。

必须按顺序执行：

### Step 1：理解需求

明确：

- 解决什么问题
- 涉及什么模块
- 哪些业务状态会改变
- 是否涉及资金
- 是否涉及订单
- 是否涉及成交
- 是否涉及 Kafka
- 是否涉及数据库
- 是否涉及 Redis

### Step 2：读取规则

依次阅读：

1. 当前模块 `AGENTS.md`
2. 根目录 `AGENTS.md`
3. 相关架构文档
4. 相关 ADR

模块级规则优先于全局通用规则。

### Step 3：搜索代码

必须先搜索已有：

- Service
- Domain Service
- Entity
- Aggregate
- Repository
- Mapper
- Consumer
- Producer
- Event
- Command
- DTO
- Exception
- Utility
- Test

优先复用已有设计，不重复造轮子。

### Step 4：分析业务链路

至少确定：

```text
入口
↓
校验
↓
领域逻辑
↓
事务边界
↓
数据库变化
↓
事件
↓
Consumer
↓
下游影响
```

### Step 5：最小正确修改

遵循：

> Minimum Correct Change

只修改完成当前任务所必要的代码。

禁止：

- 无关重构
- 无关改名
- 全项目格式化
- 擅自升级框架
- 擅自修改公共 API
- 引入没有必要的新依赖

### Step 6：验证

完成代码后必须：

```text
Compile
↓
Unit Test
↓
Integration Test
↓
Static Check
↓
Git Diff Review
```

测试失败时：

```text
分析根因
↓
修复
↓
重新测试
```

测试失败时不得声明任务完成。

---

## 5. Java 通用规范

以项目实际 Java / Spring Boot 版本为准。

优先：

- Constructor Injection
- immutable object
- 明确的领域模型
- enum 表达有限状态
- 有业务语义的异常类型

禁止：

- Field Injection
- 空 catch
- `return null` 隐藏错误
- 魔法数字
- 复制粘贴已有逻辑

---

## 6. 金额与数量类型

下列字段禁止使用：

```java
double
float
```

包括：

- price
- quantity
- amount
- balance
- fee

默认使用：

```java
BigDecimal
```

如果 Matching Engine 出于性能原因使用：

```text
long + scale
```

必须属于已经确认的架构方案。

Agent 不得自行把 `BigDecimal` 改成 `double`。

---

## 7. 数据库规则

禁止：

```sql
SELECT *
```

关键资金修改必须考虑并发。

例如余额扣减应优先使用：

```sql
UPDATE account_balance
SET available = available - :amount
WHERE user_id = :userId
  AND asset = :asset
  AND available >= :amount;
```

必须检查：

```text
affectedRows
```

禁止采用存在竞争窗口的方式：

```text
SELECT balance
↓
Java 判断余额
↓
UPDATE balance
```

---

## 8. 事务规则

属于同一数据库且必须共同成功或失败的操作，应使用本地事务保证原子性。

例如：

```text
修改余额
+
资金流水
```

禁止把以下慢操作随意放入数据库事务：

- HTTP RPC
- Kafka 阻塞等待
- 外部 API
- 文件 IO
- sleep
- 长时间计算

---

## 9. 幂等规则

任何可能被重复执行的操作都必须评估幂等性：

- HTTP 请求
- Kafka Consumer
- RPC Retry
- Scheduled Job
- Compensation Job

常见幂等键：

```text
Order:
orderId

Trade:
tradeId

Fund Flow:
tradeId + userId + bizType
```

核心金融幂等优先依赖：

```text
数据库唯一约束
+
事务
```

Redis 可以作为性能优化，但不能默认作为唯一金融一致性保障。

---

## 10. Kafka 规则

默认认为 Kafka：

```text
At-Least-Once Delivery
```

因此 Consumer 必须容忍重复消息。

不得假设：

> 一条 Kafka 消息永远只会消费一次。

数据库状态与事件发布需要一致时，优先使用：

```text
Transactional Outbox
```

禁止核心事务中简单执行：

```text
UPDATE DB
↓
producer.send()
```

然后认为数据库与 Kafka 一定一致。

---

## 11. Kafka Event 基础结构

核心事件建议至少包含：

```json
{
  "eventId": "",
  "eventType": "",
  "aggregateId": "",
  "timestamp": "",
  "version": 1,
  "traceId": "",
  "data": {}
}
```

修改 Event Schema 时必须检查：

- 向前兼容
- 向后兼容
- 老 Producer → 新 Consumer
- 新 Producer → 老 Consumer

---

## 12. 状态机规则

核心状态不得随意通过 setter 修改。

禁止类似：

```java
order.setStatus(...)
```

直接承载复杂状态流转。

应通过：

- Domain Method
- State Machine
- Command Handler

表达状态变化。

非法状态流转必须明确拒绝。

---

## 13. Retry 规则

Retry 前必须确认操作是幂等的。

数据库请求超时并不代表数据库一定没有执行成功。

因此禁止简单：

```text
catch timeout
↓
再次扣款
```

重试设计必须结合：

- businessId
- idempotency key
- unique index
- transaction
- current state

---

## 14. 日志规则

核心链路建议携带：

- traceId
- userId
- orderId
- tradeId
- symbol
- eventId
- sequence

禁止打印：

- Password
- Private Key
- API Secret
- 完整 Access Token
- 完整敏感认证数据

---

## 15. 测试规则

核心业务修改必须增加或更新对应测试。

优先覆盖：

- 正常路径
- 边界条件
- 重复请求
- 并发
- 事务失败
- 重试
- 状态非法
- Kafka 重复消费

不得为了测试通过而：

- 删除失败测试
- 注释核心代码
- 修改断言使错误结果变成正确结果

---

## 16. Code Review 优先级

Review 代码时按以下顺序：

```text
P0 资金安全
P1 数据一致性
P2 并发
P3 幂等
P4 状态机
P5 Kafka / 消息一致性
P6 可恢复性
P7 数据库性能
P8 普通代码质量
```

不得为了变量命名等低优先级问题，忽略重复扣款、状态错乱等高优先级问题。

---

## 17. Definition of Done

只有以下要求满足后才能声明完成：

```text
[ ] 已阅读相关 AGENTS.md
[ ] 已搜索现有实现
[ ] 已分析调用链
[ ] 已确认模块边界
[ ] 已分析事务
[ ] 已分析幂等
[ ] 已分析并发
[ ] 已分析状态变化
[ ] 已分析 Kafka 影响
[ ] 已完成必要代码
[ ] 已补充必要测试
[ ] 编译通过
[ ] Unit Test 通过
[ ] Integration Test 通过
[ ] 已检查 Git Diff
[ ] 无明显无关修改
```

任何核心验证失败：

```text
TASK != COMPLETE
```

---

## 18. 最终输出格式

完成任务后简要输出：

### 需求理解

说明解决的问题。

### 修改内容

说明主要修改。

### 关键设计

重点说明：

- 事务
- 幂等
- 并发
- 状态机
- Kafka

如不涉及可以省略。

### 修改文件

列出主要文件。

### 测试结果

列出实际执行的测试。

### 风险

如仍有潜在问题必须明确指出。