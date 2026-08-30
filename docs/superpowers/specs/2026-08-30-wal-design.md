# 第二阶段第三步：WAL 基础设施设计

## 目标

在不修改第一阶段 OrderBook、第二阶段撮合命令和序列机制的前提下，交付可按交易对追加、强制刷盘、文件滚动和安全读取的本地 WAL 基础设施。WAL 保存确定性的撮合命令，为后续命令处理器和恢复服务提供可靠输入。

## 已确认的约束

- 项目继续使用 Java 17、Maven、JUnit 5 和现有 Jackson 依赖。
- 第一版采用本地文件、每条记录一行 JSON、UTF-8 编码和 CRC32 校验和。
- WAL 根目录和单文件最大字节数必须可配置；默认值由后续配置类提供。
- 每个交易对独立保存日志，文件名按递增编号排序。
- append 后必须通过 `FileChannel.force(true)` 刷盘；优先保证可靠性，不实现批量刷盘或组提交。
- 本步骤不实现 MatchingCommandHandler、真实撮合、Kafka Consumer、Snapshot 或 Recovery Service。
- 不使用 double、float、随机标识或系统时间参与 WAL 记录内容。

## 包与职责

新增 `com.cex.matching.infrastructure.wal` 包：

- `WalRecord`：从 `MatchingCommand` 生成不可变的、可校验的 WAL 数据。
- `WalCodec`：以固定字段顺序编码不含校验和的规范 JSON，并使用 UTF-8 字节计算 CRC32；同时负责解码与校验。
- `WalWriter`：定义 append、flush 和关闭能力。
- `FileWalWriter`：向单个 WAL 文件追加完整记录，并在每次 append 后强制刷盘。
- `WalReader`：读取单个文件或同一交易对的全部 WAL 文件。
- `FileWalReader`：按文件编号和行号读取、解码及校验记录。
- `WalManager`：按交易对维护当前写入器，在达到字节阈值后刷盘、关闭并切换至下一文件。
- `WalException`：表示不可恢复的 WAL I/O 或状态错误。
- `WalCorruptionException`：表示非尾部记录损坏，调用方必须停止恢复。
- `WalReadResult`：返回有效记录与是否遇到可忽略不完整尾部的读取结果。

## 记录格式与校验

每一行是一个 JSON 对象，字段顺序固定为：

`sequence`、`commandId`、`orderId`、`userId`、`symbol`、`commandType`、`side`、`price`、`quantity`、`timestamp`、`checksum`。

其中 `checksum` 是对前十个字段组成的规范 JSON（不含 `checksum`）进行 UTF-8 编码后计算的 CRC32 无符号值。读取时必须使用相同字段顺序重新编码并比对校验值。这样相同 `MatchingCommand` 每次生成完全相同的 WAL 行内容。

取消命令的 `side` 写入 JSON `null`，价格和数量保持命令中的整数值。不得把整数金额转换为浮点数。

## 文件布局与滚动

文件布局为：

```text
<wal根目录>/<symbol>/wal-000001.log
<wal根目录>/<symbol>/wal-000002.log
```

`WalManager` 在写入一条完整记录前评估其 UTF-8 行字节数；如果当前非空文件加上新行会超过 `maxFileSizeBytes`，则先刷盘并关闭当前文件，再打开编号加一的文件写入该记录。单条记录即使本身大于阈值也必须完整写入空文件，不能拆分到多个文件。

文件编号采用六位十进制正整数，读取时只接受匹配 `wal-\d{6}.log` 的常规文件，并按编号升序处理。

## 生命周期与错误语义

后续命令处理器必须遵守：序列校验成功后，先 append 并 flush WAL；两者成功后才允许撮合和推进序列。WAL 写入、刷盘或关闭失败时抛出 `WalException`，调用方必须中止当前命令，不能继续撮合或提交 Kafka offset。

读取策略：

- 所有正常完整记录必须通过 JSON 解码和 CRC32 校验。
- 最后一行的 JSON 不完整、缺少换行或校验失败，视为宕机留下的不完整尾部；读取返回此前有效记录和尾部标记。
- 非最后一行的 JSON 或校验失败，抛出 `WalCorruptionException`，不能静默跳过中间命令。
- 文件编号、路径、空交易对和非法配置在构造或调用时抛出 `IllegalArgumentException`。

## 测试策略

使用 JUnit 5 和临时目录进行真实文件测试，不加载 Spring 上下文：

1. 同一条 `MatchingCommand` 编解码后字段和 CRC32 完全一致。
2. `FileWalWriter` 写入并刷盘后，`FileWalReader` 按原顺序读回记录。
3. 被篡改的中间记录抛出 `WalCorruptionException`。
4. 最后一条半写入或校验失败时保留此前记录并标记不完整尾部。
5. 小阈值触发滚动，文件编号连续，所有记录按 sequence 顺序读取。
6. `WalManager` 为不同交易对创建隔离目录和独立滚动状态。

## 范围外事项

本步骤不恢复 OrderBook、不回放命令、不创建快照、不发送 Kafka 事件、不提交 offset，也不把 WAL 代码放进撮合引擎或 Service 类。下一步 MatchingCommandHandler 负责把序列校验、WAL 和实际撮合按既定顺序连接起来。
