# SnapshotService 设计

## 目标

在不改变已建立的 WAL 优先、命令串行处理和恢复语义的前提下，为每个交易对自动创建版本化订单簿快照，限制宕机恢复时需要重放的 WAL 长度。

## 触发策略

`SnapshotService` 在每条 LIVE 命令成功写入 WAL、完成订单簿变更并推进序列后接收通知。对每个交易对独立维护：

- 自上次**成功**快照以来的已应用命令数；
- 上次**成功**快照的单调时间。

当任一条件成立时创建快照：

- 已应用命令数达到 `100_000`；
- 距上次成功快照达到 `60` 秒。

首次成功命令将初始化交易对状态；不因空闲交易对单独创建快照。默认阈值以构造参数注入，方便配置层后续接入，不在本步骤引入 Spring 配置绑定。

## 一致性与数据流

`MatchingCommandHandler` 新增可选的 `SnapshotObserver` 端口。其现有三参数构造函数保留，并使用无操作观察者，避免影响已有调用方。

在同交易对锁内处理 LIVE 命令的顺序为：

1. 校验序列并同步追加 WAL；
2. 执行内存订单簿变更；
3. 推进序列水位；
4. 通知 `SnapshotObserver`。

`SnapshotService` 是该端口的实现，依赖内存引擎的只读快照导出方法和 `SnapshotManager`。它使用刚推进的序列作为 `MatchingSnapshot.lastSequence`，并在仍持有命令处理器的交易对锁时读取订单簿；因此快照内订单集合与序列水位对应同一个确定状态。随后复用现有 `SnapshotManager.save` 的原子文件写入。

REPLAY 命令不通知观察者，恢复过程不会创建新快照或重置触发计数。

## 失败语义

快照是恢复加速器，WAL 才是恢复事实来源。快照写入异常不得导致已成功处理的 LIVE 命令对调用方表现为失败，也不得回滚序列或再次写 WAL。发生异常时：

- 保留上一份有效快照；
- 不清零命令计数或更新时间；
- 下一条成功 LIVE 命令继续满足阈值并重试写入。

由于当前模块没有日志与指标端口，本步骤不吞掉异常后静默处理：`SnapshotService` 返回一个结果值给观察者，观察者仅将结果用于后续可观测性扩展。写入失败不会抛回 `MatchingCommandHandler`。

## 领域边界

为保持 SnapshotService 不依赖 `InMemoryCommandMatchingEngine` 的实现细节，新增窄接口 `OrderBookSnapshotSource`：根据 `symbol`、`lastSequence` 和 `snapshotTimestamp` 导出不可变 `MatchingSnapshot`。内存引擎实现该接口。`SnapshotService` 只依赖该接口及 `SnapshotManager`。

## 测试

使用真实内存订单簿、`SnapshotManager` 和临时目录验证：

1. 未达到命令阈值或时间阈值不落盘；
2. 达到命令阈值创建包含正确订单、交易对和序列水位的快照；
3. 时间阈值创建快照；
4. REPLAY 不触发快照；
5. 快照写入失败不影响已应用命令和序列推进，并保留重试资格；
6. 快照观察发生在同交易对处理锁内，导出的快照序列与订单簿一致。

## 范围外

本步骤不实现 Kafka 消费者、定时空闲快照、快照保留/清理、压缩、远程备份、指标或 Spring 配置绑定；这些能力建立在本服务的端口上后续扩展。
