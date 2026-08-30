# 撮合恢复服务实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 `executing-plans` 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 从最新有效订单簿快照和后续 WAL 可靠恢复单交易对内存状态，并在成功后解除在线处理封锁。

**架构：** `MatchingRecoveryService` 负责同交易对恢复会话：先封锁在线处理并清空内存状态，按需装载快照，将 WAL 按序号排序后校验连续性并重放，最后一次性恢复序列水位。`MatchingCommandHandler` 仅暴露受包可见的恢复开始/完成通知，用来在整个恢复窗口拒绝在线命令；任何异常均不得解除封锁或推进水位。

**技术栈：** Java 17、JUnit 5、AssertJ、现有快照/WAL/命令处理组件。

---

## 文件结构

- 修改：`cex-matching-engine/src/main/java/com/cex/matching/domain/engine/InMemoryCommandMatchingEngine.java`——为恢复服务提供按交易对重置订单簿。
- 修改：`cex-matching-engine/src/main/java/com/cex/matching/application/MatchingCommandHandler.java`——提供仅 application 包可调用的成功恢复解锁操作。
- 修改：`cex-matching-engine/src/main/java/com/cex/matching/application/MatchingRecoveryService.java`——实施快照装载、WAL 过滤重放和原子水位提交。
- 创建：`cex-matching-engine/src/test/java/com/cex/matching/application/MatchingRecoveryServiceTest.java`——验证快照跳过、无快照、失败封锁和 WAL 不重复追加。

### 任务 1：恢复前清理与成功解锁边界

- [ ] **步骤 1：编写失败测试**

```java
@Test
void shouldClearExistingBookAndReplayAllWalWhenSnapshotIsAbsent() {
    engine.execute(newOrder(99L, "999"), ExecutionMode.LIVE);

    recoveryService.recover("BTC_USDT");

    assertThat(engine.findOrder("BTC_USDT", 999L)).isEmpty();
    assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
    assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(2L);
}
```

该测试应因缺少订单簿重置和恢复编排而失败。

- [ ] **步骤 2：运行失败测试**

运行：`mvn -pl cex-matching-engine -am -Dtest=MatchingRecoveryServiceTest test`

预期：FAIL，恢复后的旧订单仍存在或恢复服务尚不可实例化。

- [ ] **步骤 3：实现最小边界**

```java
public void reset(String symbol) {
    orderBooks.put(Objects.requireNonNull(symbol, "交易对不能为空"), new OrderBook(symbol));
}

void markRecoveryStarted(String symbol) {
    recoveryRequiredSymbols.add(symbol);
}

void markRecoveryCompleted(String symbol) {
    recoveryRequiredSymbols.remove(symbol);
}
```

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -pl cex-matching-engine -am -Dtest=MatchingRecoveryServiceTest test`

预期：PASS。

### 任务 2：快照基线后的有序 WAL 重放

- [ ] **步骤 1：编写失败测试**

```java
@Test
void shouldRestoreSnapshotAndReplayOnlyRecordsAfterSnapshotSequence() {
    snapshots.put("BTC_USDT", snapshotAt(2L, "11"));
    walRecords.addAll(List.of(newOrder(1L, "9"), newOrder(2L, "10"), newOrder(3L, "12")));

    recoveryService.recover("BTC_USDT");

    assertThat(engine.findOrder("BTC_USDT", 11L)).isPresent();
    assertThat(engine.findOrder("BTC_USDT", 12L)).isPresent();
    assertThat(engine.findOrder("BTC_USDT", 9L)).isEmpty();
    assertThat(sequenceManager.current("BTC_USDT")).isEqualTo(3L);
}
```

- [ ] **步骤 2：运行失败测试**

运行：`mvn -pl cex-matching-engine -am -Dtest=MatchingRecoveryServiceTest test`

预期：FAIL，未按快照水位筛选或未更新最终水位。

- [ ] **步骤 3：实现最小恢复算法**

```java
engine.reset(symbol);
long restoredSequence = snapshotManager.loadLatest(symbol)
        .map(snapshot -> { engine.restore(snapshot); return snapshot.lastSequence(); })
        .orElse(0L);
for (WalRecord record : walReader.read(symbol).records().stream()
        .sorted(comparingLong(WalRecord::sequence)).toList()) {
    if (record.sequence() > restoredSequence) {
        if (record.sequence() != restoredSequence + 1L) {
            throw new SequenceGapException(symbol, restoredSequence + 1L, record.sequence());
        }
        handler.replay(commandOf(record));
        restoredSequence = record.sequence();
    }
}
sequenceManager.restore(symbol, restoredSequence);
handler.markRecoveryCompleted(symbol);
```

- [ ] **步骤 4：运行测试验证通过**

运行：`mvn -pl cex-matching-engine -am -Dtest=MatchingRecoveryServiceTest test`

预期：PASS；测试中的 WAL 追加器保持零调用。

### 任务 3：异常路径与模块回归

- [ ] **步骤 1：编写失败测试**

```java
@Test
void shouldKeepSymbolBlockedWhenWalReplayFails() {
    walReader = symbol -> new WalReadResult(List.of(invalidNewOrder(1L)), false);

    assertThatThrownBy(() -> recoveryService.recover("BTC_USDT"))
            .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> handler.handle(newOrder(1L, "11")))
            .isInstanceOf(CommandRecoveryRequiredException.class);
}
```

- [ ] **步骤 2：运行失败测试**

运行：`mvn -pl cex-matching-engine -am -Dtest=MatchingRecoveryServiceTest test`

预期：FAIL，恢复失败后在线命令未被封锁。

- [ ] **步骤 3：限制解锁时机并补充中文 Javadoc**

仅在 `recover` 的全部重放和 `sequenceManager.restore` 成功后调用 `markRecoveryCompleted`；异常直接传播且不改变封锁集合。

- [ ] **步骤 4：运行完整验证**

运行：`mvn -pl cex-matching-engine -am test`

预期：PASS。

- [ ] **步骤 5：检查变更并提交**

```bash
git add cex-matching-engine/src/main/java/com/cex/matching/application \
        cex-matching-engine/src/main/java/com/cex/matching/domain/engine \
        cex-matching-engine/src/test/java/com/cex/matching/application \
        docs/superpowers/plans/2026-08-31-matching-recovery.md
git commit -m "feat: 支持从快照和 WAL 恢复撮合状态"
```
