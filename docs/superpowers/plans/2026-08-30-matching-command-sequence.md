# 第二阶段第一步：撮合命令与序列机制实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（- [ ]）语法来跟踪进度。

**目标：** 在 Java 17 下交付不可变撮合命令、整数金额归一化与按交易对严格校验的内存序列管理器。

**架构：** MatchingCommand 是独立传输模型，保持整数价格和数量。CommandNormalizer 通过每交易对固定小数位无损创建现有 MatchOrder，不改变第一阶段 OrderBook。InMemorySequenceManager 为每个交易对保存最后处理序列，并将重复与序列缺口显式区分。

**技术栈：** Java 17、BigDecimal、ConcurrentHashMap、JUnit Jupiter、AssertJ、Maven。

---

## 文件结构

- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/CommandType.java — 新订单与撤单命令类型。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchingCommand.java — 不可变命令及字段校验。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/application/DecimalScale.java — 交易对价格和数量小数位配置。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/application/CommandNormalizer.java — 整数命令到 MatchOrder 的精确转换。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceManager.java — 序列查询、验证与推进接口。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceValidation.java — 新序列与重复序列验证结果。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceGapException.java — 不可跳过的序列缺口异常。
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/InMemorySequenceManager.java — 按交易对内存保存与原子推进序列。
- 创建：cex-matching-engine/src/test/java/com/cex/matching/domain/model/MatchingCommandTest.java — 命令输入不变式测试。
- 创建：cex-matching-engine/src/test/java/com/cex/matching/application/CommandNormalizerTest.java — 无损整数金额转换测试。
- 创建：cex-matching-engine/src/test/java/com/cex/matching/domain/sequence/InMemorySequenceManagerTest.java — 序列连续性、重复、缺口与隔离测试。

### 任务 1：定义不可变撮合命令

**文件：**
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/CommandType.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchingCommand.java
- 测试：cex-matching-engine/src/test/java/com/cex/matching/domain/model/MatchingCommandTest.java

- [ ] **步骤 1：编写失败的新订单与撤单校验测试**

~~~java
@Test
void rejectsNonPositiveIntegerAmountForNewOrder() {
    assertThatIllegalArgumentException().isThrownBy(() -> new MatchingCommand(
            1L, "command-1", "1", "10", "BTC_USDT", CommandType.NEW_ORDER,
            MatchOrder.Side.BUY, 0L, 100_000_000L, 1_700_000_000_000L));
}

@Test
void allowsCancelCommandWithoutSideOrAmounts() {
    MatchingCommand command = new MatchingCommand(
            1L, "command-2", "1", "10", "BTC_USDT", CommandType.CANCEL_ORDER,
            null, 0L, 0L, 1_700_000_000_000L);

    assertThat(command.commandType()).isEqualTo(CommandType.CANCEL_ORDER);
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -am test

预期：FAIL，测试编译时指出 CommandType 与 MatchingCommand 不存在。

- [ ] **步骤 3：编写最少命令模型代码**

~~~java
public enum CommandType {
    NEW_ORDER,
    CANCEL_ORDER
}

public record MatchingCommand(
        long sequence, String commandId, String orderId, String userId, String symbol,
        CommandType commandType, MatchOrder.Side side, long price, long quantity, long timestamp) {

    public MatchingCommand {
        if (sequence < 1 || timestamp < 0 || blank(commandId) || blank(orderId)
                || blank(userId) || blank(symbol) || commandType == null) {
            throw new IllegalArgumentException("撮合命令字段不合法");
        }
        if (commandType == CommandType.NEW_ORDER
                && (side == null || price <= 0 || quantity <= 0)) {
            throw new IllegalArgumentException("新订单价格、数量和方向必须有效");
        }
    }
}
~~~

取消命令不要求方向、价格或数量；新订单不允许零或负整数金额。

- [ ] **步骤 4：运行命令模型测试验证通过**

运行：mvn -pl cex-matching-engine -am test

预期：PASS，非法新订单被拒绝，合法撤单可以创建。

- [ ] **步骤 5：提交命令模型交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/model/CommandType.java cex-matching-engine/src/main/java/com/cex/matching/domain/model/MatchingCommand.java cex-matching-engine/src/test/java/com/cex/matching/domain/model/MatchingCommandTest.java
git commit -m "功能：新增撮合命令模型"
~~~

### 任务 2：实现整数金额归一化

**文件：**
- 创建：cex-matching-engine/src/main/java/com/cex/matching/application/DecimalScale.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/application/CommandNormalizer.java
- 测试：cex-matching-engine/src/test/java/com/cex/matching/application/CommandNormalizerTest.java

- [ ] **步骤 1：编写失败的精确转换测试**

~~~java
@Test
void convertsIntegerAmountsWithConfiguredScalesWithoutPrecisionLoss() {
    CommandNormalizer normalizer = new CommandNormalizer(Map.of(
            "BTC_USDT", new DecimalScale(2, 8)));
    MatchingCommand command = newOrder("42", "6500012", "123456789");

    MatchOrder order = normalizer.toMatchOrder(command);

    assertThat(order.price()).isEqualByComparingTo("65000.12");
    assertThat(order.quantity()).isEqualByComparingTo("1.23456789");
    assertThat(order.remainingQuantity()).isEqualByComparingTo("1.23456789");
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -am test

预期：FAIL，测试编译时指出 CommandNormalizer 和 DecimalScale 不存在。

- [ ] **步骤 3：编写最少归一化代码**

~~~java
public record DecimalScale(int priceScale, int quantityScale) {
    public DecimalScale {
        if (priceScale < 0 || quantityScale < 0) {
            throw new IllegalArgumentException("小数位不能为负数");
        }
    }
}

public MatchOrder toMatchOrder(MatchingCommand command) {
    DecimalScale scale = scales.get(command.symbol());
    if (scale == null || command.commandType() != CommandType.NEW_ORDER) {
        throw new IllegalArgumentException("命令不能归一化为订单");
    }
    return new MatchOrder(
            Long.parseLong(command.orderId()), Long.parseLong(command.userId()),
            command.symbol(), command.side(),
            BigDecimal.valueOf(command.price(), scale.priceScale()),
            BigDecimal.valueOf(command.quantity(), scale.quantityScale()),
            BigDecimal.valueOf(command.quantity(), scale.quantityScale()),
            command.sequence());
}
~~~

构造器使用 Map.copyOf 保存小数位配置；数值身份无法转换为 long 时抛出 IllegalArgumentException。

- [ ] **步骤 4：运行归一化测试验证通过**

运行：mvn -pl cex-matching-engine -am test

预期：PASS，整数金额精确转为 BigDecimal，未发生浮点转换。

- [ ] **步骤 5：提交归一化交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/application/DecimalScale.java cex-matching-engine/src/main/java/com/cex/matching/application/CommandNormalizer.java cex-matching-engine/src/test/java/com/cex/matching/application/CommandNormalizerTest.java
git commit -m "功能：新增撮合命令金额归一化"
~~~

### 任务 3：实现按交易对的严格序列管理

**文件：**
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceManager.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceValidation.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/SequenceGapException.java
- 创建：cex-matching-engine/src/main/java/com/cex/matching/domain/sequence/InMemorySequenceManager.java
- 测试：cex-matching-engine/src/test/java/com/cex/matching/domain/sequence/InMemorySequenceManagerTest.java

- [ ] **步骤 1：编写失败的连续、重复、缺口与隔离测试**

~~~java
@Test
void acceptsNextSequenceAndKeepsSymbolsIndependent() {
    SequenceManager manager = new InMemorySequenceManager();

    assertThat(manager.validate("BTC_USDT", 1L)).isEqualTo(SequenceValidation.ACCEPT);
    manager.advance("BTC_USDT", 1L);
    assertThat(manager.current("BTC_USDT")).isEqualTo(1L);
    assertThat(manager.validate("BTC_USDT", 1L)).isEqualTo(SequenceValidation.DUPLICATE);
    assertThat(manager.validate("ETH_USDT", 1L)).isEqualTo(SequenceValidation.ACCEPT);
}

@Test
void rejectsSequenceGap() {
    SequenceManager manager = new InMemorySequenceManager();

    assertThatThrownBy(() -> manager.validate("BTC_USDT", 2L))
            .isInstanceOf(SequenceGapException.class);
}
~~~

- [ ] **步骤 2：运行测试验证失败**

运行：mvn -pl cex-matching-engine -am test

预期：FAIL，测试编译时指出 SequenceManager、SequenceValidation 和 InMemorySequenceManager 不存在。

- [ ] **步骤 3：编写最少序列管理代码**

~~~java
public interface SequenceManager {
    long current(String symbol);
    SequenceValidation validate(String symbol, long sequence);
    void advance(String symbol, long sequence);
}

public SequenceValidation validate(String symbol, long sequence) {
    long current = current(symbol);
    if (sequence <= current) return SequenceValidation.DUPLICATE;
    if (sequence == current + 1) return SequenceValidation.ACCEPT;
    throw new SequenceGapException(symbol, current, sequence);
}
~~~

InMemorySequenceManager 使用 ConcurrentHashMap<String, AtomicLong> 保存每交易对序列，并在 advance 内用 compareAndSet 保证每次只能推进一个连续序列。它不对 OrderBook 加锁，也不处理 Kafka 消费暂停。

- [ ] **步骤 4：运行完整模块测试验证通过**

运行：mvn -pl cex-matching-engine -am test

预期：PASS，命令、归一化、订单簿和序列测试均通过。

- [ ] **步骤 5：提交序列管理交付物**

~~~bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/sequence cex-matching-engine/src/test/java/com/cex/matching/domain/sequence/InMemorySequenceManagerTest.java
git commit -m "功能：新增撮合序列管理器"
~~~

## 计划自检

- 不可变命令、整数金额、固定小数位、无 double 转换、独立序列、重复忽略、缺口失败和严格推进，分别由任务 1 至任务 3 覆盖。
- 计划未修改 OrderBook，也没有引入 WAL、Snapshot、Kafka 消费、Kafka 生产、数据库或真实撮合循环。
- CommandType、MatchingCommand、DecimalScale、SequenceValidation 和 SequenceManager 的名称在所有任务中保持一致。

