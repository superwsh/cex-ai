# 第二阶段第四步：命令处理链路设计

## 目标

在不修改旧 `MatchingEngine` Kafka 事件接口的前提下，新增可靠命令处理链路，将严格序列校验、WAL 落盘、内存订单簿状态变更和序列推进连接起来。

## 架构

新增独立 `CommandMatchingEngine` 领域接口和每交易对的内存实现。`MatchingCommandHandler` 位于 application 层，负责流程编排；领域执行器只负责命令对 `OrderBook` 的确定性状态变更。

旧 `MatchingEngine.match(OrderEvent, Consumer<TradeEvent>)` 保持不变，避免将旧 Kafka 事件模型和新可靠命令模型混合。

## 执行语义

- `NEW_ORDER`：`CommandNormalizer` 将整数金额转为 `MatchOrder`，领域执行器加入对应交易对的 `OrderBook`。
- `CANCEL_ORDER`：按数值订单编号删除活动订单；订单不存在时视为幂等取消成功。
- 本步骤不新增价格撮合循环、Trade、Kafka 输出、offset 提交、Snapshot 或 Recovery Service。当前仓库尚未提供已实现的撮合执行器，后续在 `CommandMatchingEngine` 边界扩展成交即可。

## 处理顺序

LIVE 模式固定执行：

1. `SequenceManager.validate(symbol, sequence)`。
2. 重复序列直接返回 `DUPLICATE`，不写 WAL、不改订单簿、不推进序列。
3. 合法新序列转换 `WalRecord`，执行 `WalManager.append`；该调用已同步刷盘。
4. 调用 `CommandMatchingEngine.execute(command, LIVE)`。
5. 领域执行成功后调用 `SequenceManager.advance`。

WAL 或领域执行失败时不推进序列。SequenceGapException 原样传播，供后续 Kafka 消费者暂停相应分区和发起恢复。

## 执行模式

- `LIVE`：正常命令处理，经过 WAL 和序列推进；本步骤不发送外部事件。
- `REPLAY`：为恢复预留。领域执行器可执行同一状态变更，但处理器不追加 WAL、不推进已恢复序列，也不发送外部事件。

## 错误和幂等

- 无效命令、金额小数位缺失、无效订单编号等输入错误抛 IllegalArgumentException。
- WAL 失败时领域执行器不得被调用。
- 领域执行失败时 WAL 已持久化，序列保持不变，重启后由后续恢复服务重放。
- 取消不存在订单属于确定性、幂等成功，仍可推进该合法序列。

## 测试

使用真实 OrderBook、InMemorySequenceManager 和临时目录 WAL：

1. 新订单依次通过序列校验、WAL、订单簿写入和序列推进。
2. 重复命令不追加 WAL 或重复写入订单簿。
3. WAL 失败时订单簿和序列保持不变。
4. 取消存在及不存在订单均具幂等确定性。
5. REPLAY 不写 WAL、不推进序列。

## 范围外

不引入 Kafka Consumer/Producer、数据库、Redis、Snapshot、Recovery、价格撮合循环或成交输出。后续步骤仅在本设计的接口边界上扩展。
