# OrderBook 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（- [ ]）语法来跟踪进度。

**目标：** 为撮合模块交付单交易对、单线程、内存 OrderBook，并以单元测试证明价格优先、时间优先、索引与撤单行为。

**架构：** MatchOrder 是独立订单模型，嵌套 Side 枚举以避免与 Order Service 耦合。OrderBook 由倒序买盘、正序卖盘和订单索引组成；每个 PriceLevel 用 ArrayDeque 维护同价 FIFO。查询返回不可变结果。

**技术栈：** Java 17、BigDecimal、TreeMap、ArrayDeque、JUnit Jupiter、Maven。

---

## 文件结构

- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchOrder.java — 撮合订单、方向和输入不变式。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/PriceLevel.java — 单价格档位 FIFO 队列与汇总。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderLocation.java — 订单索引定位信息。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderBook.java — 买卖盘、索引、增删与查询。
- 创建：cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java — 真实对象行为测试。

### 任务 1：建立独立订单和价格档位模型

**文件：**
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchOrder.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/PriceLevel.java
- 测试：cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java

- [ ] **步骤 1：编写失败的 FIFO 与数量汇总测试**

~~~java
@Test
void samePriceOrdersAreKeptInArrivalOrder() {
    PriceLevel level = new PriceLevel(new BigDecimal("100.00"));
    MatchOrder first = order(1L, MatchOrder.Side.BUY, "100.00", "1.0");
    MatchOrder second = order(2L, MatchOrder.Side.BUY, "100.00", "2.0");

    level.addOrder(first);
    level.addOrder(second);

    assertThat(level.peekFirst()).contains(first);
    assertThat(level.totalRemainingQuantity()).isEqualByComparingTo("3.0");
    assertThat(level.removeOrder(1L)).contains(first);
    assertThat(level.peekFirst()).contains(second);
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#samePriceOrdersAreKeptInArrivalOrder test

预期：FAIL，编译信息指出 PriceLevel 与 MatchOrder 不存在。

- [ ] **步骤 3：编写最少模型代码**

~~~java
public final class MatchOrder {
    public enum Side { BUY, SELL }
    // final identity fields; validated remaining quantity
}

public final class PriceLevel {
    private final BigDecimal price;
    private final ArrayDeque<MatchOrder> orders = new ArrayDeque<>();
    public void addOrder(MatchOrder order) { orders.addLast(order); }
    public Optional<MatchOrder> peekFirst() { return Optional.ofNullable(orders.peekFirst()); }
    public Optional<MatchOrder> removeOrder(long orderId) { /* remove matching order */ }
    public BigDecimal totalRemainingQuantity() { /* sum remaining quantities */ }
}
~~~

MatchOrder 构造器拒绝空字段、非正价格/数量/剩余量及剩余量大于总量；PriceLevel.addOrder 拒绝价格不相等的订单。

- [ ] **步骤 4：运行模型测试验证通过**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#samePriceOrdersAreKeptInArrivalOrder test

预期：PASS，FIFO 头部与档位总剩余量断言成立。

- [ ] **步骤 5：提交模型交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchOrder.java cex-matching-engine/src/main/java/com/cex/matching/domain/model/PriceLevel.java cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java
git commit -m "feat: add matching order book models"
~~~

### 任务 2：实现盘口排序、订单索引和深度查询

**文件：**
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderLocation.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderBook.java
- 修改：cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java

- [ ] **步骤 1：编写失败的排序、索引和深度测试**

~~~java
@Test
void ordersUsePricePriorityAndIndexLookup() {
    OrderBook book = new OrderBook("BTC_USDT");
    book.addOrder(order(1L, MatchOrder.Side.BUY, "100.00", "1.0"));
    book.addOrder(order(2L, MatchOrder.Side.BUY, "101.00", "2.0"));
    book.addOrder(order(3L, MatchOrder.Side.SELL, "103.00", "1.5"));
    book.addOrder(order(4L, MatchOrder.Side.SELL, "102.00", "0.5"));

    assertThat(book.getBestBid()).map(PriceLevel::price).contains(new BigDecimal("101.00"));
    assertThat(book.getBestAsk()).map(PriceLevel::price).contains(new BigDecimal("102.00"));
    assertThat(book.getOrder(3L)).contains(order(3L, MatchOrder.Side.SELL, "103.00", "1.5"));
    assertThat(book.getBidDepth()).extracting(OrderBook.Depth::price)
        .containsExactly(new BigDecimal("101.00"), new BigDecimal("100.00"));
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#ordersUsePricePriorityAndIndexLookup test

预期：FAIL，编译信息指出 OrderBook、OrderLocation 和查询 API 不存在。

- [ ] **步骤 3：编写最少 OrderBook 实现**

~~~java
private final NavigableMap<BigDecimal, PriceLevel> bids =
    new TreeMap<>(Comparator.reverseOrder());
private final NavigableMap<BigDecimal, PriceLevel> asks = new TreeMap<>();
private final Map<Long, OrderLocation> orderIndex = new HashMap<>();

public void addOrder(MatchOrder order) {
    validateOrder(order);
    NavigableMap<BigDecimal, PriceLevel> side =
        order.side() == MatchOrder.Side.BUY ? bids : asks;
    PriceLevel level = side.computeIfAbsent(order.price(), PriceLevel::new);
    level.addOrder(order);
    orderIndex.put(order.orderId(), new OrderLocation(order.side(), order.price(), order));
}
~~~

实现 getBestBid、getBestAsk、getOrder、containsOrder、getBidDepth 和 getAskDepth。Depth 是不可变值对象 price、orderCount、remainingQuantity；查询返回 List.copyOf。构造时指定 symbol，addOrder 拒绝交易对不一致和重复 ID。

- [ ] **步骤 4：运行查询测试验证通过**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#ordersUsePricePriorityAndIndexLookup test

预期：PASS，买卖最佳价、索引查询和深度顺序全部成立。

- [ ] **步骤 5：提交 OrderBook 查询交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderLocation.java cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderBook.java cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java
git commit -m "feat: add indexed order book"
~~~

### 任务 3：实现撤单、价位清理与输入保护

**文件：**
- 修改：cex-matching-engine/src/main/java/com/cex/matching/domain/model/PriceLevel.java
- 修改：cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderBook.java
- 修改：cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java

- [ ] **步骤 1：编写失败的撤单与不变式测试**

~~~java
@Test
void removingLastOrderCleansPriceLevelAndIsIdempotent() {
    OrderBook book = new OrderBook("BTC_USDT");
    MatchOrder order = order(1L, MatchOrder.Side.BUY, "100.00", "1.0");
    book.addOrder(order);

    assertThat(book.removeOrder(1L)).contains(order);
    assertThat(book.getBestBid()).isEmpty();
    assertThat(book.containsOrder(1L)).isFalse();
    assertThat(book.removeOrder(1L)).isEmpty();
}

@Test
void invalidOrdersDoNotChangeBook() {
    OrderBook book = new OrderBook("BTC_USDT");
    assertThatIllegalArgumentException().isThrownBy(
        () -> book.addOrder(orderFor("ETH_USDT", 1L, MatchOrder.Side.BUY, "100.00", "1.0")));
    assertThat(book.getBidDepth()).isEmpty();
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#removingLastOrderCleansPriceLevelAndIsIdempotent,OrderBookTest#invalidOrdersDoNotChangeBook test

预期：FAIL，removeOrder 未定义、未清理空价位，或非法订单未被拒绝。

- [ ] **步骤 3：编写最少撤单与校验代码**

~~~java
public Optional<MatchOrder> removeOrder(long orderId) {
    OrderLocation location = orderIndex.remove(orderId);
    if (location == null) return Optional.empty();
    NavigableMap<BigDecimal, PriceLevel> side =
        location.side() == MatchOrder.Side.BUY ? bids : asks;
    PriceLevel level = side.get(location.price());
    MatchOrder removed = level.removeOrder(orderId)
        .orElseThrow(() -> new IllegalStateException("order index is inconsistent"));
    if (level.isEmpty()) side.remove(location.price());
    return Optional.of(removed);
}
~~~

补充 OrderBook.validateOrder 与 PriceLevel 的不一致检测，保证异常路径不修改盘口或索引。

- [ ] **步骤 4：运行模块测试验证通过**

运行：mvn -pl cex-matching-engine test

预期：PASS，所有 OrderBook 测试通过且没有 Spring 或外部服务依赖。

- [ ] **步骤 5：提交撤单交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/model/PriceLevel.java cex-matching-engine/src/main/java/com/cex/matching/domain/model/OrderBook.java cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java
git commit -m "feat: add order book cancellation"
~~~

### 任务 4：完成阶段性验证

**文件：**
- 修改：cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java

- [ ] **步骤 1：编写失败的重复 ID 回归测试**

~~~java
@Test
void duplicateIdIsRejectedWithoutChangingExistingOrder() {
    OrderBook book = new OrderBook("BTC_USDT");
    book.addOrder(order(1L, MatchOrder.Side.BUY, "100.00", "1.0"));

    assertThatIllegalArgumentException().isThrownBy(
        () -> book.addOrder(order(1L, MatchOrder.Side.SELL, "101.00", "2.0")));

    assertThat(book.getOrder(1L)).contains(order(1L, MatchOrder.Side.BUY, "100.00", "1.0"));
    assertThat(book.getAskDepth()).isEmpty();
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -Dtest=OrderBookTest#duplicateIdIsRejectedWithoutChangingExistingOrder test

预期：FAIL，重复 ID 覆盖索引、写入卖盘或未抛出异常。

- [ ] **步骤 3：补齐最少重复 ID 保护**

~~~java
if (orderIndex.containsKey(order.orderId())) {
    throw new IllegalArgumentException("duplicate active order id: " + order.orderId());
}
~~~

确认所有查询返回不可修改的集合，并保留单线程使用约束的 Javadoc。

- [ ] **步骤 4：运行完整验证**

运行：mvn -pl cex-matching-engine test
随后运行：mvn -pl cex-matching-engine -am test

预期：两个命令均为 PASS；模块与其 Maven 依赖模块均无失败测试。

- [ ] **步骤 5：提交最终交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/model cex-matching-engine/src/test/java/com/cex/matching/domain/model/OrderBookTest.java
git commit -m "test: cover order book invariants"
~~~

## 计划自检

- 规格中的独立模型、BigDecimal、单交易对、单线程、倒序买盘、正序卖盘、FIFO、索引、空价位清理、不可变查询、参数校验和测试策略，均由任务 1 至任务 4 覆盖。
- 计划没有引入撮合循环、Kafka、数据库、Redis、WAL、Snapshot、Spring 或多交易对分片。
- MatchOrder.Side 和 OrderBook.Depth 的名称在各任务中一致。

