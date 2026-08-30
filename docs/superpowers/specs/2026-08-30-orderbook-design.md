# CEX OrderBook 设计规格

## 目标

为 `cex-matching-engine` 实现第一阶段、单交易对、纯内存的 OrderBook 基础设施。该阶段只包含 `MatchOrder`、`PriceLevel`、`OrderLocation` 和 `OrderBook`，以及覆盖其行为的单元测试；不实现撮合循环、Kafka、WAL、Snapshot、数据库或 Spring 集成。

## 范围与约束

- 使用 Java 17，与仓库的构建配置保持一致。
- 订单簿核心不依赖 Spring、MySQL、Redis、Kafka，也不复用 Order Service 的持久化模型。
- 所有金额和数量使用 `BigDecimal`，不得使用 `double` 或 `float`。
- `OrderBook` 是单线程组件；调用方必须把同一交易对的所有增删操作串行化。本阶段不添加锁或并发集合。
- 同一 `OrderBook` 实例表示一个交易对；订单的 `symbol` 必须等于构造时指定的交易对。

## 模型

### MatchOrder

撮合模块独立定义的可变订单对象，包含：`orderId`、`userId`、`symbol`、`side`、`price`、`quantity`、`remainingQuantity` 和 `sequence`。本阶段只接纳限价订单，所以 `price` 必须为正数。`quantity` 与 `remainingQuantity` 必须为正数，且剩余数量不得大于总量。

`sequence` 由上游在命令顺序确定后传入；OrderBook 不使用系统时间决定订单先后。同价格订单的实际优先级由成功加入 `PriceLevel` 的 FIFO 顺序决定。

### PriceLevel

一个价格对应一个 `PriceLevel`。它持有该价格及 `ArrayDeque<MatchOrder>`：

- `addOrder` 将订单追加到队尾；
- `peekFirst` 返回最早入簿的订单；
- `removeOrder(orderId)` 在该价位内删除指定订单并返回删除结果；
- `totalRemainingQuantity` 汇总该价位中订单的剩余数量；
- 只暴露不可变的订单快照，避免外部修改 FIFO 队列。

### OrderLocation

订单索引值，保存订单方向、价格与订单引用。它让撤单操作从按全簿扫描变为：`orderId -> location -> price level -> order`。

### OrderBook

`OrderBook` 保存指定交易对的以下状态：

- `bids`：`TreeMap<BigDecimal, PriceLevel>`，使用倒序比较器，最高买价位于首位；
- `asks`：`TreeMap<BigDecimal, PriceLevel>`，使用自然升序，最低卖价位于首位；
- `orderIndex`：`Map<Long, OrderLocation>`，保存全部活动订单的位置。

不允许多个 `OrderBook` 之间共享可变订单对象。

## API 与行为

### addOrder

`addOrder(MatchOrder order)` 校验订单非空、交易对匹配、订单 ID 未存在、方向存在、价格/数量/剩余数量均合法。校验通过后，按方向选择买盘或卖盘，在价格不存在时新建 `PriceLevel`，将订单追加到队尾，并写入索引。

相同价格严格按成功调用 `addOrder` 的顺序排列；买卖盘的价格优先级分别由倒序和升序 `TreeMap` 保证。重复订单 ID 以明确的业务异常拒绝，且不得修改任何订单簿状态。

### removeOrder

`removeOrder(long orderId)` 先查索引。不存在时返回 `Optional.empty()`，不抛异常；存在时从对应 `PriceLevel` 删除订单、从索引删除位置，并在价位为空时从相应 `TreeMap` 删除该价位。成功时返回被删除的订单。

重复撤同一订单与撤不存在订单行为一致，均返回空结果。这一幂等语义只适用于本地 OrderBook 状态；终态订单的业务状态判断属于后续撮合引擎职责。

### 查询

- `getBestBid()`：返回最高买价的 `PriceLevel` 不可变快照；无买盘时为空。
- `getBestAsk()`：返回最低卖价的 `PriceLevel` 不可变快照；无卖盘时为空。
- `getOrder(orderId)`：通过索引返回订单快照；不存在时为空。
- `containsOrder(orderId)`：仅根据索引判断活动订单是否存在。
- `getBidDepth()` 与 `getAskDepth()`：返回按价格优先顺序排列的不可变深度列表；每项包含价格、该价位活动订单数和剩余数量之和。

查询 API 不暴露内部 `TreeMap`、`ArrayDeque` 或可修改集合。

## 错误处理

订单输入不合法（交易对不匹配、ID 重复、方向为空、非正价格/数量、剩余量大于总量）属于调用方错误，抛出参数或领域校验异常。OrderBook 不把此类错误伪装成成交拒绝事件；事件模型属于后续撮合阶段。

内部结构不一致（例如索引存在但价位或订单不存在）视为系统数据损坏，应抛出 `IllegalStateException`，不能静默修复或继续处理。

## 测试策略

在 `cex-matching-engine` 模块中新增 JUnit 测试，以真实 OrderBook 对象验证：

1. 买卖订单分别进入正确盘口，且最佳买价最高、最佳卖价最低。
2. 同价订单按添加顺序保持 FIFO。
3. 不同价格的深度按买盘降序、卖盘升序排列，并正确聚合剩余数量。
4. `getOrder`、`containsOrder` 使用索引定位活动订单。
5. 删除订单会同步删除索引；移除价位最后一笔订单时自动清理该价位。
6. 不存在订单与重复撤单返回空结果且不破坏状态。
7. 重复订单 ID、交易对不匹配、非法金额和非法剩余量被拒绝且不写入订单簿。

测试不引入 Spring 上下文，不依赖外部服务。

## 后续衔接

OrderBook 通过本规格的全部测试后，下一项独立工作是实现限价订单撮合：使用最佳对手价位、按 FIFO 取 maker、以 maker 价格生成成交，并维护剩余量。IOC、FOK、市场单、订单事件与命令队列均不属于当前交付。
