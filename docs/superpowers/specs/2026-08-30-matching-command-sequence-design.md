# 第二阶段第一步：撮合命令与序列机制设计

## 目标

在保持 Java 17 和第一阶段 BigDecimal OrderBook 不变的前提下，建立第二阶段可靠撮合入口所需的统一命令模型与每交易对严格序列控制。本步骤只交付命令模型、整数金额归一化与 SequenceManager；不实现 WAL、快照、恢复、Kafka 消费者、事件生产者或真实撮合。

## 已确认的约束

- 项目保持 Java 17，不升级全仓库。
- 匹配核心继续使用第一阶段的 MatchOrder 和 BigDecimal OrderBook。
- 传输层 MatchingCommand 的价格、数量使用 long 整数；不得使用 double 或 float。
- 每个交易对独立维护序列，撮合状态仍由单写线程修改。
- 命令时间戳由上游传入；本步骤不使用系统时间、随机数或随机标识参与业务结果。

## 模型边界

### MatchingCommand

新增不可变 MatchingCommand，字段包括：

- sequence：上游为同一交易对分配的严格递增序列号。
- commandId：上游提供的确定性命令标识。
- orderId、userId、symbol：订单身份和交易对。
- commandType：NEW_ORDER 或 CANCEL_ORDER。
- side：买入或卖出；取消命令可不携带。
- price、quantity：long 整数化金额；新订单必须为正，取消命令可以为零。
- timestamp：上游事件时间。

MatchingCommand 只表达传输命令，不依赖 Kafka DTO、数据库实体或订单服务模型。

### CommandNormalizer

新增 CommandNormalizer，构造时接收每个交易对固定的 priceScale 和 quantityScale。对于新订单，使用 BigDecimal.valueOf(long, scale) 将整数价格和数量无损转换为现有 MatchOrder 所需的 BigDecimal；序列和命令时间戳由后续撮合引擎保留。取消命令不创建 MatchOrder。

为避免改变第一阶段的订单簿接口，本步骤不把 OrderBook 内部价格和数量改为 long。

### SequenceManager

新增 SequenceManager 接口及内存实现，每个 symbol 独立维护 lastProcessedSequence。

- current(symbol) 返回当前已处理序列；未初始化交易对返回 0。
- validate(symbol, sequence) 不修改状态。
- sequence 小于等于当前值时返回重复结果，调用方必须忽略该命令。
- sequence 等于当前值加一时允许处理。
- sequence 大于当前值加一时抛出 SequenceGapException。
- advance(symbol, sequence) 仅接受当前值加一；其他序列抛出状态异常。

SequenceManager 不推进重复命令，不自动修补序列缺口，也不直接暂停 Kafka 消费。后续 MatchingCommandHandler 将根据重复和缺口结果决定是否处理命令、暂停消费和发起恢复。

## 数据流

正常命令的后续处理顺序固定为：

1. 调用 SequenceManager.validate。
2. 重复命令直接结束，不写 WAL、不撮合、不推进序列。
3. 合法新序列进入后续 WAL 写入与刷新。
4. WAL 成功后才能调用撮合逻辑。
5. 撮合成功后调用 SequenceManager.advance。
6. 后续阶段再发布结果事件并提交 Kafka offset。

本步骤只实现第 1、2 和第 5 项所需的类型与行为，不实现后续基础设施。

## 错误处理

- 非法命令字段、非正的新订单价格或数量、负序列、空交易对和空命令标识属于输入错误，抛出 IllegalArgumentException。
- SequenceGapException 表示可靠性故障，不能当作订单拒绝或静默跳过。
- advance 的非连续序列表示本地状态不一致，抛出 IllegalStateException。
- CANCEL_ORDER 仅校验身份、交易对、序列和时间戳，不要求 side、price 或 quantity。

## 测试策略

新增纯 JUnit 测试，不加载 Spring 上下文：

1. MatchingCommand 拒绝非法新订单整数金额，允许合法取消命令。
2. CommandNormalizer 按固定小数位精确转换价格与数量，不发生 double 精度损失。
3. 首个序列从 1 开始可以处理和推进。
4. 连续序列可以处理；重复序列被标记为重复且不推进。
5. 存在序列缺口时抛出 SequenceGapException。
6. 不同交易对的序列完全隔离。
7. advance 只能推进下一个连续序列。

## 后续衔接

只有本步骤全部测试通过后，才进入第二步 WAL 的数据结构、写入、校验和读取。WAL 接入时，MatchingCommandHandler 必须先完成序列验证，再追加并刷盘 WAL，最后才可调用撮合逻辑。

