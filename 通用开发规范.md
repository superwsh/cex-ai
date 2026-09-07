# Java Backend / CEX 项目统一开发规范

## 1. 总体原则

所有代码必须遵循以下原则：

* 优先保证正确性，其次考虑性能，最后考虑代码简洁。
* 不允许为了“少写代码”牺牲可读性、可维护性和业务正确性。
* 所有核心业务逻辑必须显式表达，不允许依赖隐式副作用。
* 所有涉及资金、订单、交易、清算的数据变更必须考虑：

    * 原子性
    * 幂等性
    * 一致性
    * 可追溯性
    * 可恢复性
* 不允许在没有充分理由的情况下引入新的框架、中间件或依赖。
* 修改现有代码时，优先遵循当前项目已有架构和编码风格，不随意重构无关代码。

---

# 2. 技术栈规范

默认技术栈：

* Java 17
* Spring Boot 3.x
* Spring MVC
* Spring Validation
* MyBatis / MyBatis-Plus
* MySQL 8.x
* Redis
* Kafka
* Maven
* JUnit 5
* Mockito

除非项目已有明确技术选型，否则不要随意替换技术栈。

例如：

* 不要无理由把 MyBatis 改成 JPA。
* 不要无理由引入 Reactor。
* 不要无理由使用 Redis Stream 替代 Kafka。
* 不要无理由新增 MQ、数据库、中间件。

---

# 3. 项目分层规范

推荐模块结构：

```text
controller
application
domain
infrastructure
repository
mapper
entity
dto
vo
config
common
exception
```

职责要求：

## controller

只负责：

* 接收请求
* 参数校验
* 身份信息获取
* 调用 application/service
* 返回结果

禁止：

* 编写核心业务逻辑
* 直接操作数据库
* 直接操作 Mapper

---

## application / service

负责：

* 业务流程编排
* 事务边界
* 多领域对象协调
* 调用 domain / repository

核心业务流程必须放在此层或 domain 层。

---

## domain

负责核心业务规则，例如：

* 订单状态转换
* 撮合规则
* 成交逻辑
* 余额计算
* 手续费计算
* 风控规则

核心领域规则不能散落在 Controller、Mapper 中。

---

## repository

负责数据访问抽象。

例如：

```java
public interface OrderRepository {

    Order findById(Long orderId);

    void save(Order order);

    boolean updateStatus(
        Long orderId,
        OrderStatus from,
        OrderStatus to
    );
}
```

---

## mapper

只负责 SQL。

禁止在 Mapper 中包含业务决策。

---

# 4. 命名规范

类名使用大驼峰：

```text
OrderService
OrderController
MatchingEngine
SettlementService
```

方法名使用小驼峰：

```text
createOrder
cancelOrder
matchOrder
freezeBalance
```

布尔变量必须体现语义：

推荐：

```java
isFinished
hasEnoughBalance
canCancel
```

不推荐：

```java
flag
statusFlag
result
```

集合名称必须使用复数：

```java
orders
trades
users
```

对象需要 Builder 时，优先直接在构造器或类上使用 Lombok `@Builder`。

禁止无业务必要地手写 Builder 内部类。只有生成的 Builder 无法表达必要的构建逻辑时才允许自定义，并必须在代码注释中说明原因。

---

# 5. 方法设计规范

一个方法只负责一个核心职责。

原则上：

* 方法不超过 50 行。
* 参数尽量不超过 5 个。
* 超过 5 个参数优先封装对象。
* 避免超过 3 层嵌套。
* 每个方法要提供中文注释，说明其功能以及参数。
* 方法中的实现代码根据复杂度适当添加注释，注意不要过度添加注释。

推荐：

```java
public void createOrder(CreateOrderCommand command) {
    validateOrder(command);
    freezeBalance(command);
    saveOrder(command);
    publishOrderCreatedEvent(command);
}
```

而不是把所有逻辑写在一个 300 行的方法里。

---

# 6. 参数校验规范

Controller 使用 Bean Validation：

```java
@NotNull
@Positive
private BigDecimal price;
```

业务级校验必须在 Service / Domain 中再次判断。

例如：

* 交易对是否存在
* 交易对是否可交易
* 用户状态是否正常
* 余额是否充足
* 最小下单金额
* 最大下单数量
* Price Tick
* Quantity Step

不允许只依赖前端校验。

---

# 7. BigDecimal 规范

资金、价格、数量禁止使用：

```java
double
float
```

必须使用：

```java
BigDecimal
```

BigDecimal 比较必须使用：

```java
a.compareTo(b)
```

禁止：

```java
a.equals(b)
```

金额计算必须明确 roundingMode。

例如：

```java
amount.setScale(8, RoundingMode.DOWN);
```

禁止使用未指定精度的除法。

---

# 8. 数据库规范

表名：

```text
order
trade
account
account_flow
```

推荐实际业务使用带业务域前缀的名称，例如：

```text
cex_order
cex_trade
cex_account
cex_account_flow
```

字段使用 snake_case：

```text
user_id
order_id
created_at
updated_at
```

所有业务表原则上必须包含：

```text
id
created_at
updated_at
```

关键业务记录根据需要增加：

```text
version
status
```

---

# 9. 数据库主键规范

内部数据库主键建议使用：

```text
BIGINT
```

业务 ID 与数据库主键可以分离。

例如：

```text
id
order_id
trade_id
```

order_id / trade_id 应具备全局唯一性。

---

# 10. 索引规范

所有高频查询必须有索引。

例如订单查询：

```text
(user_id, symbol, created_at)
```

幂等数据必须通过数据库唯一索引兜底。

例如：

```text
UNIQUE(order_id)
```

资金流水：

```text
UNIQUE(trade_id, user_id, biz_type)
```

禁止只依赖 Redis 实现最终幂等。

---

# 11. SQL 规范

禁止：

```sql
SELECT *
```

必须明确字段。

余额更新必须通过 SQL 原子判断。

例如：

```sql
UPDATE account
SET available = available - #{amount},
    frozen = frozen + #{amount}
WHERE user_id = #{userId}
  AND asset = #{asset}
  AND available >= #{amount};
```

必须检查 affected rows。

如果：

```text
affectedRows = 0
```

应认为余额不足或状态冲突。

禁止：

1. select balance
2. Java 判断
3. update balance

因为存在并发问题。

---

# 12. 事务规范

事务必须尽可能小。

资金操作必须保证：

```text
余额修改
+
资金流水
```

处于同一事务。

例如：

```text
BEGIN

UPDATE account

INSERT account_flow

COMMIT
```

禁止：

```text
更新余额成功
↓
事务提交
↓
再写流水
```

否则可能出现账实不一致。

---

# 13. 订单规范

订单必须有明确状态机。

例如：

```text
NEW
↓
PENDING_FREEZE
↓
OPEN
↓
PARTIALLY_FILLED
↓
FILLED
```

取消：

```text
OPEN
↓
CANCELING
↓
CANCELED
```

异常：

```text
REJECTED
```

禁止任意状态直接修改。

状态更新建议使用 CAS：

```sql
UPDATE cex_order
SET status = 'CANCELED'
WHERE order_id = ?
AND status = 'OPEN';
```

必须检查 affected rows。

---

# 14. Kafka 使用规范

Kafka 用于：

* Order Created
* Order Cancel
* Trade Created
* Settlement
* Market Data
* Order Book Update

Topic 命名：

```text
cex.order.created
cex.order.cancel
cex.trade.created
cex.settlement
cex.market.trade
cex.market.depth
```

生产消息必须考虑：

```text
acks=all
```

消费者必须支持：

```text
At Least Once
+
业务幂等
```

不要依赖 Kafka Exactly Once 解决所有业务一致性问题。

---

# 15. Kafka Key 规范

撮合消息：

```text
key = symbol
```

例如：

```text
BTC-USDT
ETH-USDT
```

确保：

```text
同一个 symbol
→ 同一个 partition
→ 单线程顺序消费
```

保证订单进入撮合引擎的顺序。

---

# 16. Kafka Consumer 规范

Consumer 必须考虑：

* 重复消费
* 消费失败
* Retry
* Dead Letter Queue
* 消费积压
* Rebalance

消费逻辑必须幂等。

例如：

```text
收到 TradeCreated
↓
检查 trade_id 是否已处理
↓
已处理
→ 直接 ACK

未处理
→ 执行业务
→ 记录处理状态
→ ACK
```

---

# 17. Outbox 规范

数据库变更和 Kafka 事件需要一致时，优先使用 Transactional Outbox。

事务：

```text
BEGIN

INSERT order

UPDATE account

INSERT outbox_event

COMMIT
```

异步任务读取：

```text
outbox_event
↓
Kafka
```

发送成功后：

```text
status = SENT
```

必须支持失败重试。

---

# 18. 幂等规范

所有跨服务写操作必须考虑幂等。

常见 ID：

```text
request_id
order_id
trade_id
event_id
```

数据库唯一索引作为最终防线。

例如：

```sql
UNIQUE(event_id)
```

禁止只写：

```java
if (redis.exists(key)) {
    return;
}
```

然后认为已经完全解决幂等问题。

---

# 19. 撮合引擎规范

撮合采用：

```text
Price Priority
+
Time Priority
```

买单：

```text
价格从高到低
```

卖单：

```text
价格从低到高
```

同价格：

```text
FIFO
```

推荐：

```text
TreeMap<Price, PriceLevel>
```

PriceLevel：

```text
Deque<Order>
```

订单快速定位：

```text
HashMap<OrderId, OrderReference>
```

---

# 20. 撮合线程模型

原则：

```text
一个 symbol
=
一个逻辑撮合线程
```

避免多个线程同时修改同一个 OrderBook。

禁止为了“提高并发”给一个订单簿增加大量锁。

优先通过：

```text
Symbol Partition
```

实现横向扩展。

---

# 21. 撮合核心代码规范

撮合核心路径禁止：

* 调数据库
* 调 Redis
* RPC
* HTTP
* 写磁盘同步 IO

撮合线程应主要操作：

```text
内存
```

结果通过 Event 输出。

---

# 22. 撮合恢复规范

撮合引擎必须支持：

```text
Snapshot
+
WAL / Event Log
```

启动：

```text
读取 Snapshot
↓
恢复 OrderBook
↓
获取 snapshotSequence
↓
重放 sequence > snapshotSequence 的事件
```

保证恢复一致性。

---

# 23. 资金系统规范

账户建议至少维护：

```text
available
frozen
```

冻结：

```text
available -= amount
frozen += amount
```

解冻：

```text
frozen -= amount
available += amount
```

成交：

```text
frozen -= amount
```

每一次资金变更必须有流水。

---

# 24. 资金流水规范

资金流水必须包含：

```text
user_id
asset
biz_type
biz_id
amount
before_available
after_available
before_frozen
after_frozen
created_at
```

必须能够根据流水进行：

```text
Audit
Reconciliation
Recovery
```

---

# 25. 异常处理规范

统一业务异常：

```java
BizException
```

例如：

```java
throw new BizException(
    ErrorCode.INSUFFICIENT_BALANCE
);
```

禁止：

```java
throw new RuntimeException("余额不足");
```

禁止把 Java 堆栈直接返回客户端。

---

# 26. API 返回规范

统一：

```json
{
  "code": "0",
  "message": "success",
  "data": {}
}
```

错误：

```json
{
  "code": "ORDER_NOT_FOUND",
  "message": "order not found"
}
```

---

# 27. 日志规范

关键日志必须包含业务 ID：

```text
requestId
userId
orderId
tradeId
symbol
```

例如：

```java
log.info(
    "order created, userId={}, orderId={}, symbol={}",
    userId,
    orderId,
    symbol
);
```

禁止：

```java
log.info("order created");
```

资金、订单、成交异常必须具备完整上下文。

---

# 28. 日志安全

禁止打印：

* Password
* Private Key
* API Secret
* JWT 完整 Token
* 身份证
* 银行卡
* 用户敏感信息

API Key 只能脱敏打印。

---

# 29. 并发规范

优先避免共享状态。

如果可以使用：

```text
单线程
分区
Actor
Queue
```

解决，则不要优先使用锁。

禁止随意添加：

```java
synchronized
```

任何锁必须说明：

```text
保护什么资源
为什么需要
锁粒度
可能的性能影响
```

---

# 30. Redis 规范

Redis 适合：

* Cache
* Rate Limit
* Distributed Lock
* Session
* 临时状态

Redis 不应该作为：

```text
资金最终事实来源
```

资金最终状态必须有数据库或可靠账本保证。

---

# 31. 分布式锁规范

能通过数据库 CAS / 唯一索引解决的问题，不优先使用分布式锁。

优先级：

```text
单线程
>
数据库唯一索引
>
CAS
>
Redis Lock
```

---

# 32. 单元测试规范

核心业务必须测试。

包括：

```text
订单创建
订单取消
资金冻结
资金解冻
完全成交
部分成交
重复消费
余额不足
重复订单
撮合价格优先
撮合时间优先
```

修改核心业务代码时必须同步修改测试。

---

# 33. 测试命名规范

推荐：

```java
shouldCreateOrderWhenBalanceEnough()
```

```java
shouldRejectOrderWhenBalanceInsufficient()
```

```java
shouldMatchHigherBuyPriceFirst()
```

测试应该体现：

```text
条件
+
行为
+
预期结果
```

---

# 34. AI 编码规范

AI 修改代码之前必须先：

1. 阅读相关代码。
2. 理解当前项目结构。
3. 查找已有类似实现。
4. 确认影响范围。
5. 再进行修改。

禁止：

```text
没有阅读代码直接生成整套新架构
```

---

# 35. AI 修改原则

AI 必须遵循：

```text
Minimal Change
```

只修改完成当前任务必要的文件。

禁止：

* 顺手重构整个模块。
* 修改无关格式。
* 大规模重命名。
* 删除看似无用但未确认用途的代码。

---

# 36. AI 编码前分析

开始编码前输出：

```text
需求理解
影响模块
实现方案
风险点
计划修改文件
```

如果是简单修改，可以简化，但不得完全跳过分析。

---

# 37. AI 编码后检查

代码完成后必须检查：

```text
编译
测试
边界条件
异常路径
事务
并发
幂等
数据库索引
Kafka 重复消费
```

并说明：

```text
修改了什么
为什么这么修改
是否存在剩余风险
```

---

# 38. 禁止事项

禁止：

* 无理由引入新依赖。
* 无理由修改公共 API。
* 使用 double 表示资金。
* 先查余额再更新余额。
* 忽略 SQL affected rows。
* Kafka Consumer 不做幂等。
* 状态机任意跳转。
* Controller 写核心业务。
* 捕获 Exception 后什么都不做。
* 使用空 catch。
* 把异常堆栈直接返回用户。
* 硬编码密码、Token、Secret。
* 删除失败测试以让 CI 通过。
* 为了通过测试修改错误业务逻辑。

---

# 39. Codex 工作方式

收到需求后按照下面流程执行：

```text
1. 阅读 AGENTS.md
2. 阅读相关模块代码
3. 查找类似实现
4. 明确现有架构
5. 给出修改方案
6. 实现代码
7. 编译
8. 执行测试
9. 修复问题
10. 输出修改总结
```

除非用户明确要求，否则不要跳过测试。

---

# 40. 设计优先级

在出现多个方案时，按照以下顺序选择：

```text
正确性
>
数据一致性
>
可恢复性
>
可维护性
>
性能
>
代码简洁
```

对于撮合核心路径：

```text
正确性
>
确定性
>
性能
>
扩展性
```

对于资金系统：

```text
正确性
>
一致性
>
可审计性
>
性能
```

任何涉及订单、成交、资金的代码，都必须优先保证数据正确性。
