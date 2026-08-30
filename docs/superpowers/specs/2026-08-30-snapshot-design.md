# 第二阶段第五步：Snapshot 设计

## 目标

为每个交易对持久化与 `lastSequence` 一致的完整活动订单簿，缩短后续 WAL 重放时间。

## 数据模型

`MatchingSnapshot` 保存 `symbol`、`lastSequence`、`snapshotTimestamp` 以及买卖订单列表。订单保留 `orderId`、`userId`、`side`、`price`、`quantity`、`remainingQuantity` 和 `sequence`，全部使用现有 `MatchOrder` 的数值语义，避免精度丢失。

## 文件与原子性

每个交易对使用 `<root>/<symbol>/snapshot-<lastSequence>.json`。写入时先创建同目录唯一 `.tmp` 文件，完整写入后强制刷盘，再以 `ATOMIC_MOVE` 重命名；不支持原子替换的文件系统视为失败，不降级为非原子覆盖。读取按序列降序尝试，校验 JSON 与模型字段，损坏文件跳过并回退到较旧的有效快照。

## 一致性边界

Snapshot 不直接并发读取 `OrderBook`。`MatchingCommandHandler` 将在与 LIVE 命令相同的交易对锁内调用 Snapshot 服务，并传入该交易对当前序列；因此快照记录的序列和订单簿状态处于同一安全点。不同交易对仍可并行快照。

## 范围

本步只提供 Snapshot 模型、读写器和管理器接口。命令计数/定时触发、从 Snapshot 恢复订单簿、WAL 重放和 Kafka 生命周期留给第六步 Recovery。

## 测试

验证买卖订单与序列的往返、原子临时文件清理、损坏最新文件回退、非法路径拒绝和空订单簿快照。
