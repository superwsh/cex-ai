# Market Data System — Phase 1–9

## 1. 当前项目分析

- 订单服务通过 `cex.order.event` 发送订单命令；撮合引擎按 `symbol` 键分区，并为每个交易对维护连续的命令序号。
- 撮合引擎已发布 `cex.trade.event`，其中 `TradeEvent` 使用 `BigDecimal` 表示价格、数量和成交额；清算服务以 `eventId` 幂等，并依赖既有字段完成结算。
- 撮合内存订单簿使用价格优先、时间优先。行情服务不能读取该内存对象，必须从事件重建自己的 Level 2 聚合盘口。
- `cex-market-service` 已有 Spring Boot、Redis、Kafka 和 Netty 依赖，但在本阶段之前没有行情领域模型、消费者或持久化逻辑。

## 2. 本阶段模块结构

```text
cex-common-kafka
└── event
    ├── TradeEvent
    └── market
        ├── OrderBookDeltaEvent
        └── PriceLevelChange

cex-market-service
└── domain
    ├── model
    │   ├── MarketOrderBook
    │   ├── MarketDataStatus
    │   └── DeltaApplyResult
    └── exception
        └── MarketSequenceGapException
```

## 3. 事件协议

### TradeEvent v1

保留现有清算契约：`eventId`、`tradeId`、`sequence`、`symbol`、买卖订单和用户、资产、`price`、`quantity`、`amount`、手续费及 `timestamp`。Phase 1 新增：

- `eventVersion`：默认值为 `1`，旧 JSON 未携带该字段时仍按 v1 读取。
- `quoteQuantity`：与既有 `amount` 相同，供行情语义明确地表示计价成交量；`amount` 不删除。
- `takerSide`：`BUY` 或 `SELL`，由撮合记录的 maker 方向确定。
- `createdAt`：事件创建时间。当前映射与撮合成交时间一致，避免重放时产生不确定值。

金额继续沿用项目的 `BigDecimal`，不引入浮点数或新的固定点编码。未来新增字段必须保持向后兼容；破坏性变更使用新的 `eventVersion` 并让消费者显式支持。

### OrderBookDeltaEvent v1

主题：`cex.matching.book-delta`。

必填字段为 `eventId`、`eventVersion`、`symbol`、`sequence`、`previousSequence`、`bids`、`asks` 和 `eventTime`。可选 `sourceSequence` 记录触发此次变化的撮合命令序号，只用于追溯。价格档位使用 `PriceLevelChange(price, quantity)`：数量大于零表示写入该聚合档位，零表示删除，负数非法。`bids` 按价格高到低读取，`asks` 按价格低到高读取。

## 4. Sequence、幂等与恢复方案

每个 `symbol` 独立维护盘口 sequence；它是撮合端在每次实际盘口变更时递增的独立序列，不能复用 `TradeEvent.sequence`（后者是撮合命令序号，一条命令可能产生多笔成交或没有盘口变化）。`sourceSequence` 只保留两者关联。撮合端必须以 `symbol` 作为 Kafka key，因此同一交易对由同一分区顺序投递。行情端在其单交易对执行器中检查：

```text
event.sequence <= localSequence         -> 视为 Kafka 重复/旧事件，忽略
event.previousSequence == localSequence -> 应用增量并推进 sequence
其余情况                              -> 标记 INVALID，停止增量，进入恢复
```

恢复协调器（下一阶段）须执行：`INVALID -> RECOVERING -> 加载权威 Snapshot(sequence=N) -> 重放 N 后的 delta -> ACTIVE`。在 `INIT`、`RECOVERING`、`INVALID` 或 `SUSPENDED` 状态下，不允许推送或应用增量。盘口模型仅由同一 symbol 的单一消费者修改，不依靠细粒度锁。

## 5. Kafka 与版本方案

| 数据 | Topic | Partition key | 生产者 | 消费者 |
| --- | --- | --- | --- | --- |
| 成交 | `cex.trade.event` | `symbol` | Matching Engine | Clearing、Market、Notification |
| 盘口增量 | `cex.matching.book-delta` | `symbol` | Matching Engine（下一阶段接入） | Market |

本阶段只定义新增 topic 常量，不创建 Kafka topic、不改 consumer 配置，也不启动生产者。部署时两个 topic 都必须保证同一 symbol 使用稳定键和顺序分区。

## 6. Phase 2：Market Order Book

Phase 2 在 Phase 1 的增量应用模型之上补齐了：

- 买方高价优先、卖方低价优先的 `bestBid()` / `bestAsk()` 查询；
- `MarketDepthSnapshot` 不可变领域快照，包含交易对、盘口序号、买卖档位和快照时间；
- 深度快照的防御性复制，确保后续盘口增量不会改变已生成快照。

该快照是内存读取模型，尚不承担 Redis、数据库或恢复持久化职责。

## 7. Phase 3：Trade + Ticker

- `MarketTradeConsumer` 消费 `cex.trade.event`，以 Kafka 的 `symbol` 分区顺序进入行情应用层。
- 每个交易对维护最多 1,000 笔最近成交、最新价和最多 100,000 个进程内成交幂等键。
- `Ticker24hRollingWindow` 使用分钟桶聚合最近 24 小时的开高低收、基础/计价成交量和笔数；涨跌幅按 `HALF_UP` 保留 8 位小数。分钟级任务会推进窗口并刷新缓存，避免无成交时统计过期。
- `MarketBookTickerApplicationService` 从 `MarketOrderBook` 的最优档位构建 BookTicker；盘口 Delta 消费者在后续阶段调用它。
- Redis 写入 `market:trades:{symbol}`、`market:lastPrice:{symbol}`、`market:ticker:{symbol}` 和 `market:bookTicker:{symbol}`。Redis 仅为可重建热点缓存。

## 8. Phase 4：KLine

- 支持 `1m`、`5m`、`15m`、`1h`、`4h` 和 `1d`，全部以 UTC Epoch 固定窗口切分。
- 当前未收线 KLine 写入 Redis：`market:kline:{symbol}:{interval}`；窗口滚动时，上根 KLine 使用 MySQL 唯一索引 `(symbol, interval, open_time)` 幂等 upsert。
- 提供 `GET /api/v1/market/klines`，参数为 `symbol`、`interval`、可选 `startTime`、`endTime`、`limit`（默认 500、最大 1000）。
- 数据库初始化脚本为 `docker/mysql/init/11-market-phase4-kline.sql`；已存在 MySQL volume 的环境需手动执行该脚本。

## 9. Phase 5：REST Market API

所有接口统一返回 `ApiResult`：成功时 `code=0`，参数不合法为 `400`，单交易对行情尚未就绪为 `404`，触发请求限制为 `429`。

| 接口 | 说明 |
| --- | --- |
| `GET /api/v1/market/symbols` | 已接收到实际成交或盘口数据的交易对 |
| `GET /api/v1/market/ticker/24hr?symbol=BTC_USDT` | 单交易对 24 小时 Ticker；省略 `symbol` 查询全部已就绪 Ticker |
| `GET /api/v1/market/bookTicker?symbol=BTC_USDT` | 单交易对最优买卖报价；省略 `symbol` 查询全部已就绪报价 |
| `GET /api/v1/market/depth?symbol=BTC_USDT&limit=20` | 指定交易对深度；仅支持 5、10、20、50、100、500 档 |
| `GET /api/v1/market/trades?symbol=BTC_USDT&limit=100` | 最近成交，最大 1,000 条 |
| `GET /api/v1/market/klines?...` | 历史与当前 KLine |
| `GET /api/v1/market/time` | 服务 UTC 毫秒时间戳 |

交易对由实际行情写入时登记到 `market:symbols`，而非复制订单服务的交易对配置；因此列表只表示“已有行情数据”的交易对，不是下单资格的权威来源。盘口刷新时，服务同步将至多 500 档深度写入 `market:depth:{symbol}`。

公开市场接口使用 Redis 固定窗口按客户端连接 IP 限流，默认 60 秒 120 次；可用 `MARKET_RATE_LIMIT_REQUESTS`、`MARKET_RATE_LIMIT_WINDOW_SECONDS` 覆盖。网关部署时应保证客户端 IP 的可信传递，避免所有流量被识别为同一个反向代理地址。

## 10. Phase 6：WebSocket

Netty 在 `ws://{host}:9001/ws` 提供行情长连接。握手完成后服务在内存中保存“连接 -> 频道集合”；连接关闭即删除全部订阅，不写数据库或 Redis。

客户端协议：

```json
{"id":"req-1","op":"SUBSCRIBE","channels":["trade.BTC_USDT","kline.BTC_USDT.1m"]}
{"id":"req-2","op":"UNSUBSCRIBE","channels":["trade.BTC_USDT"]}
{"id":"ping-1","op":"PING"}
```

支持频道：`trade.{symbol}`、`ticker.{symbol}`、`bookTicker.{symbol}`、`depth.{symbol}`、`kline.{symbol}.{interval}`。`interval` 为 `1m`、`5m`、`15m`、`1h`、`4h`、`1d`。单连接最多 100 个频道。

服务端应答为 `ACK`、`ERROR` 或 `PONG`；行情推送统一为：

```json
{"type":"TRADE","channel":"trade.BTC_USDT","data":{},"serverTime":1700000000000}
```

WebSocket 读空闲超时默认为 90 秒，客户端应定期发送应用层 `PING` 或标准 WebSocket Ping 帧；可通过 `MARKET_WS_HEARTBEAT_TIMEOUT_SECONDS` 调整。Trade、Ticker、KLine 在成交/KLine 处理确认后推送，BookTicker 与 Depth 在盘口刷新并写入缓存后推送。

## 11. Phase 7：行情广播优化

WebSocket 连接不再在每次发布时遍历全部会话，而是维护“频道 -> 订阅连接”索引。每条待广播消息只序列化一次，使用 Netty 引用计数副本扇出给频道订阅者。

- Trade 会在短周期内按频道聚合，发送 `TRADE_BATCH`，其 `data` 是保序的成交数组；默认最多 100 笔，刷新周期为 50ms。
- Ticker、BookTicker、Depth、KLine 在一个刷新周期内按频道合并，只发送最后一份快照。
- 逐笔成交待处理队列默认上限 10,000。队列满时对生产线程施加背压，待批量刷新腾出容量后继续入队，避免静默丢失或打乱同交易对成交顺序。
- 不可写连接不再继续写入 Netty 出站缓冲；连续 3 个广播周期不可写即关闭连接。客户端可通过 REST 快照和重新订阅恢复。

可通过 `MARKET_WS_FLUSH_INTERVAL_MS`、`MARKET_WS_TRADE_BATCH_SIZE`、`MARKET_WS_MAX_PENDING_TRADES`、`MARKET_WS_MAX_CONSECUTIVE_UNWRITABLE` 调整。

## 12. Phase 8：Snapshot & Recovery

`cex.matching.book-delta` 现由行情服务消费。每个成功应用的盘口增量都会先将完整订单簿、交易对 sequence、Kafka partition 和 offset 以交易对唯一键写入 `market_order_book_snapshot`，再更新 BookTicker、Depth 与 WebSocket；快照写失败会使 Kafka 当前记录不提交。

启动时，服务读取全部快照并执行状态转换 `INIT -> RECOVERING -> ACTIVE`，恢复为可继续应用增量的内存订单簿。重复或旧 sequence 被忽略。发生 sequence gap 时，工作副本进入 `INVALID`，恢复器基于最后持久化快照创建 `RECOVERING` 订单簿，并使用独立 Kafka Consumer 从 `snapshot.kafka_offset + 1` 重放到当前记录位点；仅重放成功到目标 sequence 才替换活动订单簿并继续推送。

新增数据库脚本为 `docker/mysql/init/12-market-phase8-orderbook-snapshot.sql`。已有 MySQL volume 需要手动执行该脚本。Kafka 重放超时默认为 10 秒，可通过 `MARKET_RECOVERY_REPLAY_TIMEOUT_MS` 调整。

## 13. Phase 9：监控

Prometheus 指标通过 `GET /actuator/prometheus` 暴露（Actuator 与应用使用相同 HTTP 端口）。本阶段新增的 Micrometer 指标如下：

| 指标 | 标签 | 含义 |
| --- | --- | --- |
| `cex.market.websocket.connections` | 无 | 当前活跃 WebSocket 连接数 |
| `cex.market.websocket.slow_consumer_disconnect` | 无 | 因持续不可写而主动断开的慢客户端次数 |
| `cex.market.orderbook.sequence_gap` | 无 | 检测到的盘口 sequence gap 次数 |
| `cex.market.orderbook.recovery` | `result=success/failure` | gap 触发的盘口恢复结果 |
| `cex.market.event.delay` | `event_type=trade/order_book_delta` | 事件产生时间到行情处理完成的延迟 Timer |
| `cex.market.kafka.lag` | `topic`、`partition` | 消费组 `cex-market` 每个已提交分区的真实 Kafka lag |

Kafka lag 由独立 Kafka AdminClient 每 30 秒查询一次，不参与消费者提交或订单簿处理；查询异常只记录告警，绝不阻塞行情消费。可用 `MARKET_KAFKA_LAG_ENABLED`、`MARKET_KAFKA_LAG_INTERVAL_MS` 和 `MARKET_KAFKA_LAG_TIMEOUT_MS` 调整。事件延迟仅用于监控，不影响重放、快照或行情事实数据。

## 14. 文件清单与边界

当前已完成公共盘口事件、行情订单簿、最佳买卖盘、订单簿快照与 Kafka 重放恢复、逐笔成交消费、最近成交、最新价、BookTicker、24 小时 Ticker、KLine 聚合、KLine 持久化、市场 REST 查询、带批量/合并/慢客户端保护的 WebSocket 推送，以及 Prometheus 监控。撮合端盘口 Delta 生产与专用压测工具尚未接入。
