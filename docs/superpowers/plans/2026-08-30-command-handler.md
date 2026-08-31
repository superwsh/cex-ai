# 命令处理链路实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 `executing-plans` 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在单个交易对的顺序执行模型中，将命令校验、WAL 落盘、订单簿变更和序列推进组成可靠链路。

**架构：** 新增独立的 `CommandMatchingEngine`，使旧 Kafka `MatchingEngine` 保持不变。`MatchingCommandHandler` 仅负责编排：LIVE 模式先校验序列并同步追加 WAL，再执行内存领域状态变更，成功后推进序列；REPLAY 模式只执行领域状态变更。用 `WalAppender` 将处理器与按交易对管理文件的 `WalManager` 隔离，便于验证 WAL 失败路径。

**技术栈：** Java 17、JUnit 5、AssertJ、现有 `OrderBook`、`InMemorySequenceManager`、JSON Lines WAL。

---

## 文件结构

- 新建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/ExecutionMode.java`——区分 LIVE 与 REPLAY 执行语义。
- 新建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/CommandMatchingEngine.java`——命令驱动的领域执行接口。
- 新建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/InMemoryCommandMatchingEngine.java`——按交易对维护 `OrderBook` 的单线程实现。
- 新建：`cex-matching-engine/src/main/java/com/cex/matching/infrastructure/wal/WalAppender.java`——处理器依赖的 WAL 追加端口。
- 修改：`cex-matching-engine/src/main/java/com/cex/matching/infrastructure/wal/WalManager.java`——实现 `WalAppender`。
- 新建：`cex-matching-engine/src/main/java/com/cex/matching/application/CommandHandlingResult.java`——处理结果：已应用或重复。
- 新建：`cex-matching-engine/src/main/java/com/cex/matching/application/MatchingCommandHandler.java`——编排序列、WAL 与领域执行。
- 新建：`cex-matching-engine/src/test/java/com/cex/matching/domain/engine/InMemoryCommandMatchingEngineTest.java`——验证新增与幂等取消的领域行为。
- 新建：`cex-matching-engine/src/test/java/com/cex/matching/application/MatchingCommandHandlerTest.java`——验证处理顺序、重复、失败和重放。

### 任务 1：命令驱动的内存订单簿执行器

**文件：**
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/ExecutionMode.java`
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/CommandMatchingEngine.java`
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/InMemoryCommandMatchingEngine.java`
- 测试：`cex-matching-engine/src/test/java/com/cex/matching/domain/engine/InMemoryCommandMatchingEngineTest.java`

- [ ] **步骤 1：编写失败的领域测试**

```java
@Test
void shouldAddNormalizedOrderForNewOrderCommand() {
    InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(normalizer());

    engine.execute(newOrder(1L, "11"), ExecutionMode.LIVE);

    assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
}

@Test
void shouldTreatMissingCancelAsIdempotentSuccess() {
    InMemoryCommandMatchingEngine engine = new InMemoryCommandMatchingEngine(normalizer());

    engine.execute(cancelOrder(1L, "99"), ExecutionMode.LIVE);

    assertThat(engine.findOrder("BTC_USDT", 99L)).isEmpty();
}
```

- [ ] **步骤 2：运行领域测试验证失败**

运行：`mvn -pl cex-matching-engine -Dtest=InMemoryCommandMatchingEngineTest test`

预期：FAIL，缺少命令执行器类型。

- [ ] **步骤 3：实现最少领域模型**

```java
public interface CommandMatchingEngine {
    void execute(MatchingCommand command, ExecutionMode executionMode);
}

public void execute(MatchingCommand command, ExecutionMode executionMode) {
    Objects.requireNonNull(command, "撮合命令不能为空");
    Objects.requireNonNull(executionMode, "执行模式不能为空");
    if (command.commandType() == CommandType.NEW_ORDER) {
        bookOf(command.symbol()).addOrder(commandNormalizer.toMatchOrder(command));
        return;
    }
    bookOf(command.symbol()).removeOrder(parseOrderId(command.orderId()));
}
```

实现 `findOrder(symbol, orderId)` 仅用于读取当前内存状态；所有公开方法和参数使用中文 Javadoc。

- [ ] **步骤 4：运行领域测试验证通过**

运行：`mvn -pl cex-matching-engine -Dtest=InMemoryCommandMatchingEngineTest test`

预期：PASS。

- [ ] **步骤 5：提交领域执行器**

```bash
git add cex-matching-engine/src/main/java/com/cex/matching/domain/engine cex-matching-engine/src/test/java/com/cex/matching/domain/engine
git commit -m "feat: 新增命令驱动内存撮合执行器"
```

### 任务 2：WAL 追加端口与命令处理器

**文件：**
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/infrastructure/wal/WalAppender.java`
- 修改：`cex-matching-engine/src/main/java/com/cex/matching/infrastructure/wal/WalManager.java`
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/application/CommandHandlingResult.java`
- 创建：`cex-matching-engine/src/main/java/com/cex/matching/application/MatchingCommandHandler.java`
- 测试：`cex-matching-engine/src/test/java/com/cex/matching/application/MatchingCommandHandlerTest.java`

- [ ] **步骤 1：编写失败的应用层测试**

```java
@Test
void shouldPersistThenExecuteThenAdvanceForAcceptedCommand() {
    handler.handle(newOrder(1L, "11"));

    assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(1L);
    assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
    assertThat(appendedRecords).extracting(WalRecord::sequence).containsExactly(1L);
}

@Test
void shouldNotExecuteOrAdvanceWhenWalAppendFails() {
    MatchingCommandHandler failedHandler = handler(record -> {
        throw new WalException("写入失败");
    });

    assertThatThrownBy(() -> failedHandler.handle(newOrder(1L, "11")))
            .isInstanceOf(WalException.class);
    assertThat(sequenceManager.current("BTC_USDT")).isZero();
    assertThat(engine.findOrder("BTC_USDT", 11L)).isEmpty();
}
```

覆盖重复命令不产生副作用、取消存在/不存在订单，以及 `replay(command)` 不追加 WAL 且不推进序列。

- [ ] **步骤 2：运行应用层测试验证失败**

运行：`mvn -pl cex-matching-engine -Dtest=MatchingCommandHandlerTest test`

预期：FAIL，缺少 WAL 端口和处理器类型。

- [ ] **步骤 3：实现 WAL 端口和固定处理顺序**

```java
public CommandHandlingResult handle(MatchingCommand command) {
    if (sequenceManager.validate(command.symbol(), command.sequence()) == SequenceValidation.DUPLICATE) {
        return CommandHandlingResult.DUPLICATE;
    }
    walAppender.append(WalRecord.from(command));
    commandMatchingEngine.execute(command, ExecutionMode.LIVE);
    sequenceManager.advance(command.symbol(), command.sequence());
    return CommandHandlingResult.APPLIED;
}

public CommandHandlingResult replay(MatchingCommand command) {
    commandMatchingEngine.execute(command, ExecutionMode.REPLAY);
    return CommandHandlingResult.APPLIED;
}
```

`WalManager` 声明为 `implements WalAppender`，无需改变其现有落盘与滚动行为。所有新增方法提供中文 Javadoc 和参数说明。

- [ ] **步骤 4：运行应用层测试验证通过**

运行：`mvn -pl cex-matching-engine -Dtest=MatchingCommandHandlerTest test`

预期：PASS。

- [ ] **步骤 5：提交命令处理链路**

```bash
git add cex-matching-engine/src/main/java/com/cex/matching/application cex-matching-engine/src/main/java/com/cex/matching/infrastructure/wal cex-matching-engine/src/test/java/com/cex/matching/application
git commit -m "feat: 新增可靠撮合命令处理链路"
```

### 任务 3：模块回归验证

**文件：**
- 修改：仅当回归测试暴露本步骤新增代码缺陷时修改对应实现或测试文件。

- [ ] **步骤 1：运行撮合引擎模块全量测试**

运行：`mvn -pl cex-matching-engine -am test`

预期：PASS，所有既有订单簿、序列和 WAL 测试继续通过。

- [ ] **步骤 2：检查工作区与提交范围**

运行：`git status --short` 和 `git log --oneline -3`

预期：本步骤只有两个功能提交；不暂存或修改用户归档文档以外的文件。

