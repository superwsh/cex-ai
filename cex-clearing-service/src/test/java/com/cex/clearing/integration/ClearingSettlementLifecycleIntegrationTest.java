package com.cex.clearing.integration;

import com.cex.clearing.application.observability.ClearingAlertService;
import com.cex.clearing.application.observability.ClearingMetrics;
import com.cex.clearing.application.service.AccountCommandApplicationService;
import com.cex.clearing.application.service.SettlementTaskApplicationService;
import com.cex.clearing.application.service.TradeEventValidator;
import com.cex.clearing.application.service.TradeSettlementApplicationService;
import com.cex.clearing.domain.clearing.ClearingCalculator;
import com.cex.clearing.domain.clearing.TradeEventFeeCalculator;
import com.cex.clearing.infrastructure.kafka.TradeEventRetryPublisher;
import com.cex.clearing.infrastructure.persistence.entity.AccountBalancePO;
import com.cex.clearing.infrastructure.persistence.entity.AccountOperationPO;
import com.cex.clearing.infrastructure.persistence.entity.BalanceFlowPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementEventOutboxPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalEntryPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementJournalPO;
import com.cex.clearing.infrastructure.persistence.entity.SettlementTaskPO;
import com.cex.clearing.infrastructure.persistence.mapper.AccountBalanceMapper;
import com.cex.clearing.infrastructure.persistence.mapper.AccountOperationMapper;
import com.cex.clearing.infrastructure.persistence.mapper.BalanceFlowMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementEventOutboxMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalEntryMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementJournalMapper;
import com.cex.clearing.infrastructure.persistence.mapper.SettlementTaskMapper;
import com.cex.clearing.interfaces.consumer.OrderUnfreezeConsumer;
import com.cex.clearing.interfaces.consumer.TradeExecutedConsumer;
import com.cex.common.dubbo.account.FreezeBalanceRequest;
import com.cex.common.kafka.event.OrderUnfreezeEvent;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.common.kafka.event.TradeSettledEvent;
import com.cex.matching.application.mapper.TradeEventMapper;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchResult;
import com.cex.matching.domain.model.OrderBook;
import com.cex.matching.domain.service.InMemoryMatchingEngine;
import com.cex.order.application.service.OrderEventConsumer;
import com.cex.order.application.service.OrderUnfreezeEventPublisher;
import com.cex.order.application.service.SymbolConfigService;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderFactory;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.cex.order.infrastructure.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Phase 11 跨模块组件集成测试，验证现货成交从订单创建到最终解冻的完整业务链。 */
class ClearingSettlementLifecycleIntegrationTest {

    private static final long BUY_ORDER_ID = 11001L;
    private static final long SELL_ORDER_ID = 11002L;
    private static final long BUYER_USER_ID = 101L;
    private static final long SELLER_USER_ID = 202L;
    private static final String SYMBOL = "BTC-USDT";
    private static final BigDecimal BUY_PRICE = new BigDecimal("100000");
    private static final BigDecimal TRADE_PRICE = new BigDecimal("99000");
    private static final BigDecimal QUANTITY = new BigDecimal("0.1");

    /**
     * 跑通创建、冻结、撮合、Kafka 消费边界、清算、账本、Outbox、订单更新和解冻，
     * 并验证相同成交与解冻事件重复十次不会重复改变资金。
     */
    @Test
    void shouldCompleteSettlementLifecycleAndRemainIdempotentForDuplicateEvents() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        SymbolConfig symbolConfig = createSymbolConfig();
        Order buyerOrder = createOrder(BUY_ORDER_ID, BUYER_USER_ID, OrderSide.BUY, BUY_PRICE);
        Order sellerOrder = createOrder(SELL_ORDER_ID, SELLER_USER_ID, OrderSide.SELL, TRADE_PRICE);
        InMemoryPersistence persistence = new InMemoryPersistence(objectMapper);

        freezeOrderFunds(persistence, buyerOrder, sellerOrder, symbolConfig);
        TradeEvent tradeEvent = matchAndMapTrade();
        TradeExecutedConsumer tradeConsumer = persistence.createTradeConsumer();
        tradeConsumer.onTradeExecuted(tradeEvent);

        assertSettlementResult(persistence, tradeEvent);
        assertDuplicateTradeIsIdempotent(persistence, tradeConsumer, tradeEvent);

        OrderEventConsumer orderConsumer = persistence.createOrderConsumer(buyerOrder, sellerOrder, symbolConfig);
        TradeSettledEvent settledEvent = persistence.readTradeSettledEvent();
        orderConsumer.onTradeSettledEvent(settledEvent);
        orderConsumer.onTradeSettledEvent(settledEvent);

        assertThat(buyerOrder.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(sellerOrder.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(persistence.orderOutboxes).hasSize(1);

        OrderUnfreezeEvent unfreezeEvent = persistence.readOrderUnfreezeEvent();
        OrderUnfreezeConsumer unfreezeConsumer = new OrderUnfreezeConsumer(persistence.accountCommandService);
        unfreezeConsumer.onOrderUnfreeze(unfreezeEvent);
        for (int index = 0; index < 10; index++) {
            unfreezeConsumer.onOrderUnfreeze(unfreezeEvent);
        }

        assertFinalBalancesAndLedger(persistence);
    }

    /** 创建固定精度的可交易 BTC-USDT 配置。 */
    private SymbolConfig createSymbolConfig() {
        return SymbolConfig.builder().symbol(SYMBOL).baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(8).minQuantity(new BigDecimal("0.0001"))
                .minAmount(new BigDecimal("10")).status(SymbolConfig.SymbolStatus.ACTIVE).build();
    }

    /** 使用订单领域工厂创建一笔待撮合限价单。 */
    private Order createOrder(long orderId, long userId, OrderSide side, BigDecimal price) {
        return new OrderFactory().createPendingMatchOrder(orderId, userId, "phase11-" + orderId,
                SYMBOL, side, OrderType.LIMIT, price, QUANTITY, null, TimeInForce.GTC);
    }

    /** 通过真实账户命令服务冻结买卖双方下单资金。 */
    private void freezeOrderFunds(InMemoryPersistence persistence, Order buyerOrder, Order sellerOrder,
                                  SymbolConfig symbolConfig) {
        FreezeCalculator calculator = new FreezeCalculator();
        BigDecimal buyerFreeze = calculator.calculate(buyerOrder.getSide(), buyerOrder.getType(),
                buyerOrder.getPrice(), buyerOrder.getQuantity(), buyerOrder.getQuoteAmount());
        BigDecimal sellerFreeze = calculator.calculate(sellerOrder.getSide(), sellerOrder.getType(),
                sellerOrder.getPrice(), sellerOrder.getQuantity(), sellerOrder.getQuoteAmount());
        persistence.accountCommandService.freeze(new FreezeBalanceRequest(BUYER_USER_ID,
                calculator.freezeCurrency(OrderSide.BUY, symbolConfig), buyerFreeze, "FREEZE_ORDER",
                String.valueOf(BUY_ORDER_ID)));
        persistence.accountCommandService.freeze(new FreezeBalanceRequest(SELLER_USER_ID,
                calculator.freezeCurrency(OrderSide.SELL, symbolConfig), sellerFreeze, "FREEZE_ORDER",
                String.valueOf(SELL_ORDER_ID)));
        assertBalance(persistence, BUYER_USER_ID, "USDT", "0", "10000");
        assertBalance(persistence, SELLER_USER_ID, "BTC", "0", "0.1");
    }

    /** 使用真实内存撮合引擎成交，并映射为清算 Kafka 公共事件。 */
    private TradeEvent matchAndMapTrade() {
        InMemoryMatchingEngine engine = new InMemoryMatchingEngine(new OrderBook(SYMBOL), () -> 1L);
        engine.process(toMatchOrder(SELL_ORDER_ID, SELLER_USER_ID,
                com.cex.matching.domain.enums.OrderSide.SELL, TRADE_PRICE, 1L));
        MatchResult result = engine.process(toMatchOrder(BUY_ORDER_ID, BUYER_USER_ID,
                com.cex.matching.domain.enums.OrderSide.BUY, BUY_PRICE, 2L));
        assertThat(result.getTrades()).hasSize(1);
        assertThat(result.getTrades().get(0).getPrice()).isEqualByComparingTo(TRADE_PRICE);
        return new TradeEventMapper().toTradeEvent(result.getTrades().get(0));
    }

    /** 构造撮合引擎使用的限价订单。 */
    private MatchOrder toMatchOrder(long orderId, long userId,
                                    com.cex.matching.domain.enums.OrderSide side, BigDecimal price, long sequence) {
        return MatchOrder.builder().orderId(orderId).userId(userId).symbol(SYMBOL)
                .baseAsset("BTC").quoteAsset("USDT").side(side)
                .type(com.cex.matching.domain.enums.OrderType.LIMIT).price(price).quantity(QUANTITY)
                .timeInForce(com.cex.matching.domain.enums.TimeInForce.GTC)
                .createdAt(Instant.parse("2026-08-31T00:00:00Z")).sequence(sequence).build();
    }

    /** 校验清算后的余额、流水、Journal、任务状态与事务 Outbox。 */
    private void assertSettlementResult(InMemoryPersistence persistence, TradeEvent tradeEvent) {
        assertThat(persistence.task.get().getStatus()).isEqualTo("SUCCESS");
        assertBalance(persistence, BUYER_USER_ID, "USDT", "0", "100");
        assertBalance(persistence, BUYER_USER_ID, "BTC", "0.1", "0");
        assertBalance(persistence, SELLER_USER_ID, "BTC", "0", "0");
        assertBalance(persistence, SELLER_USER_ID, "USDT", "9900", "0");
        assertThat(persistence.balanceFlows.stream()
                .filter(flow -> "TRADE_SETTLEMENT".equals(flow.getBizType())).toList()).hasSize(4);
        assertThat(persistence.journals).hasSize(1);
        assertThat(persistence.journalEntries).hasSize(4);
        assertThat(persistence.settlementOutboxes).hasSize(1);
        assertThat(persistence.settlementOutboxes.get(0).getAggregateId()).isEqualTo(tradeEvent.getTradeId());
        assertThat(persistence.settlementOutboxes.get(0).getStatus()).isEqualTo("NEW");
    }

    /** 重复投递同一成交十次，校验数据库幂等状态阻止重复过账。 */
    private void assertDuplicateTradeIsIdempotent(InMemoryPersistence persistence,
                                                   TradeExecutedConsumer consumer, TradeEvent event) {
        List<String> balanceSnapshot = persistence.balanceSnapshot();
        for (int index = 0; index < 10; index++) {
            consumer.onTradeExecuted(event);
        }
        assertThat(persistence.balanceSnapshot()).isEqualTo(balanceSnapshot);
        assertThat(persistence.settlementOutboxes).hasSize(1);
        assertThat(persistence.balanceFlows.stream()
                .filter(flow -> "TRADE_SETTLEMENT".equals(flow.getBizType())).toList()).hasSize(4);
    }

    /** 校验价格改善释放后的最终余额、资产守恒及全链路资金流水。 */
    private void assertFinalBalancesAndLedger(InMemoryPersistence persistence) {
        assertBalance(persistence, BUYER_USER_ID, "USDT", "100", "0");
        assertBalance(persistence, BUYER_USER_ID, "BTC", "0.1", "0");
        assertBalance(persistence, SELLER_USER_ID, "USDT", "9900", "0");
        assertBalance(persistence, SELLER_USER_ID, "BTC", "0", "0");
        assertThat(persistence.balanceFlows).hasSize(7);
        assertThat(persistence.balance(BUYER_USER_ID, "USDT").getAvailable()
                .add(persistence.balance(SELLER_USER_ID, "USDT").getAvailable()))
                .isEqualByComparingTo("10000");
        assertThat(persistence.balance(BUYER_USER_ID, "BTC").getAvailable()
                .add(persistence.balance(SELLER_USER_ID, "BTC").getAvailable()))
                .isEqualByComparingTo("0.1");
    }

    /** 校验指定账户的可用和冻结余额。 */
    private void assertBalance(InMemoryPersistence persistence, long userId, String asset,
                               String available, String frozen) {
        AccountBalancePO balance = persistence.balance(userId, asset);
        assertThat(balance.getAvailable()).isEqualByComparingTo(available);
        assertThat(balance.getFrozen()).isEqualByComparingTo(frozen);
    }

    /**
     * 用有状态 Mapper 替身承载跨模块组件测试；生产服务、领域规则、消费者和序列化均使用真实实现，
     * 数据库事务与 Kafka Broker 语义由真实基础设施验收测试负责。
     */
    private static final class InMemoryPersistence {
        private final ObjectMapper objectMapper;
        private final Map<AccountKey, AccountBalancePO> balances = new HashMap<>();
        private final Set<String> accountOperations = new HashSet<>();
        private final List<BalanceFlowPO> balanceFlows = new ArrayList<>();
        private final List<SettlementJournalPO> journals = new ArrayList<>();
        private final List<SettlementJournalEntryPO> journalEntries = new ArrayList<>();
        private final List<SettlementEventOutboxPO> settlementOutboxes = new ArrayList<>();
        private final List<OrderEventOutboxPO> orderOutboxes = new ArrayList<>();
        private final AtomicReference<SettlementTaskPO> task = new AtomicReference<>();
        private final AtomicReference<AccountKey> lastChangedAccount = new AtomicReference<>();
        private final AccountBalanceMapper accountBalanceMapper = mock(AccountBalanceMapper.class);
        private final AccountOperationMapper accountOperationMapper = mock(AccountOperationMapper.class);
        private final BalanceFlowMapper balanceFlowMapper = mock(BalanceFlowMapper.class);
        private final SettlementTaskMapper settlementTaskMapper = mock(SettlementTaskMapper.class);
        private final SettlementJournalMapper journalMapper = mock(SettlementJournalMapper.class);
        private final SettlementJournalEntryMapper journalEntryMapper = mock(SettlementJournalEntryMapper.class);
        private final SettlementEventOutboxMapper settlementOutboxMapper = mock(SettlementEventOutboxMapper.class);
        private final AccountCommandApplicationService accountCommandService;

        /** 初始化四个相关资产账户及所有有状态 Mapper 行为。 */
        private InMemoryPersistence(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            putBalance(BUYER_USER_ID, "USDT", "10000", "0");
            putBalance(BUYER_USER_ID, "BTC", "0", "0");
            putBalance(SELLER_USER_ID, "USDT", "0", "0");
            putBalance(SELLER_USER_ID, "BTC", "0.1", "0");
            configureAccountMappers();
            configureSettlementMappers();
            accountCommandService = new AccountCommandApplicationService(
                    accountBalanceMapper, accountOperationMapper, balanceFlowMapper);
        }

        /** 创建包含真实建档、校验、结算和指标逻辑的 Kafka 成交消费者。 */
        private TradeExecutedConsumer createTradeConsumer() {
            TradeSettlementApplicationService settlementService = new TradeSettlementApplicationService(
                    settlementTaskMapper, accountBalanceMapper, balanceFlowMapper, journalMapper,
                    journalEntryMapper, settlementOutboxMapper,
                    new ClearingCalculator(new TradeEventFeeCalculator()), objectMapper);
            SettlementTaskApplicationService taskService = new SettlementTaskApplicationService(
                    settlementTaskMapper, new TradeEventValidator());
            ClearingMetrics metrics = new ClearingMetrics(new SimpleMeterRegistry());
            return new TradeExecutedConsumer(taskService, settlementService, mock(TradeEventRetryPublisher.class),
                    metrics, new ClearingAlertService(metrics));
        }

        /** 创建真实订单成交消费者，并让订单 Repository 与 Outbox 使用有状态替身。 */
        private OrderEventConsumer createOrderConsumer(Order buyerOrder, Order sellerOrder,
                                                       SymbolConfig symbolConfig) {
            Map<Long, Order> orders = Map.of(buyerOrder.getOrderId(), buyerOrder, sellerOrder.getOrderId(), sellerOrder);
            OrderRepository orderRepository = mock(OrderRepository.class);
            when(orderRepository.findByOrderId(any())).thenAnswer(invocation -> orders.get(invocation.getArgument(0)));
            ProcessedEventRepository processedRepository = processedEventRepository();
            SymbolConfigService configService = mock(SymbolConfigService.class);
            when(configService.getRequired(SYMBOL)).thenReturn(symbolConfig);
            OutboxRepository outboxRepository = mock(OutboxRepository.class);
            doAnswer(invocation -> orderOutboxes.add(invocation.getArgument(0)))
                    .when(outboxRepository).insert(any(OrderEventOutboxPO.class));
            OrderUnfreezeEventPublisher publisher = new OrderUnfreezeEventPublisher(outboxRepository,
                    new SnowflakeGenerator(1, 1), objectMapper, new FreezeCalculator());
            return new OrderEventConsumer(orderRepository, processedRepository, configService, publisher);
        }

        /** 创建订单消费者的内存幂等仓储。 */
        private ProcessedEventRepository processedEventRepository() {
            ProcessedEventRepository repository = mock(ProcessedEventRepository.class);
            Set<String> processedEvents = new HashSet<>();
            when(repository.exists(anyString(), eq(OrderEventConsumer.CONSUMER)))
                    .thenAnswer(invocation -> processedEvents.contains(invocation.getArgument(0)));
            doAnswer(invocation -> {
                ProcessedEventPO event = invocation.getArgument(0);
                processedEvents.add(event.getEventId());
                return null;
            }).when(repository).save(any(ProcessedEventPO.class));
            return repository;
        }

        /** 反序列化清算事务 Outbox 中的订单更新事件。 */
        private TradeSettledEvent readTradeSettledEvent() throws Exception {
            return objectMapper.readValue(settlementOutboxes.get(0).getPayload(), TradeSettledEvent.class);
        }

        /** 反序列化订单终态事务 Outbox 中的资金解冻事件。 */
        private OrderUnfreezeEvent readOrderUnfreezeEvent() throws Exception {
            return objectMapper.readValue(orderOutboxes.get(0).getPayload(), OrderUnfreezeEvent.class);
        }

        /** 配置冻结、解冻、清算余额更新和资金流水持久化行为。 */
        private void configureAccountMappers() {
            when(accountOperationMapper.insert(any(AccountOperationPO.class))).thenAnswer(invocation -> {
                AccountOperationPO operation = invocation.getArgument(0);
                String key = operation.getOperationType() + ':' + operation.getUserId() + ':'
                        + operation.getAsset() + ':' + operation.getBizType() + ':' + operation.getBizId();
                if (!accountOperations.add(key)) {
                    throw new DuplicateKeyException("duplicate account operation");
                }
                return 1;
            });
            when(accountBalanceMapper.freeze(any(), anyString(), any())).thenAnswer(invocation ->
                    changeFrozen(invocation.getArgument(0), invocation.getArgument(1),
                            invocation.getArgument(2), true));
            when(accountBalanceMapper.unfreeze(any(), anyString(), any())).thenAnswer(invocation ->
                    changeFrozen(invocation.getArgument(0), invocation.getArgument(1),
                            invocation.getArgument(2), false));
            when(accountBalanceMapper.applyChange(any(), anyString(), any(), any())).thenAnswer(invocation ->
                    applyChange(invocation.getArgument(0), invocation.getArgument(1),
                            invocation.getArgument(2), invocation.getArgument(3)));
            when(accountBalanceMapper.selectOne(any())).thenAnswer(invocation -> copy(balance(
                    lastChangedAccount.get().userId(), lastChangedAccount.get().asset())));
            when(accountBalanceMapper.selectList(any())).thenAnswer(invocation ->
                    balances.values().stream().map(InMemoryPersistence::copy).toList());
            when(balanceFlowMapper.insert(any(BalanceFlowPO.class))).thenAnswer(invocation -> {
                balanceFlows.add(invocation.getArgument(0));
                return 1;
            });
        }

        /** 配置结算任务状态机、Journal 和清算 Outbox 持久化行为。 */
        private void configureSettlementMappers() {
            when(settlementTaskMapper.selectOne(any())).thenAnswer(invocation -> task.get());
            when(settlementTaskMapper.insert(any(SettlementTaskPO.class))).thenAnswer(invocation -> {
                task.set(invocation.getArgument(0));
                return 1;
            });
            when(settlementTaskMapper.claimForSettlement(anyString())).thenAnswer(invocation -> {
                SettlementTaskPO current = task.get();
                if (current == null || !("INIT".equals(current.getStatus()) || "RETRY".equals(current.getStatus()))) {
                    return 0;
                }
                current.setStatus("PROCESSING");
                return 1;
            });
            when(settlementTaskMapper.markSuccess(anyString())).thenAnswer(invocation -> {
                if (task.get() == null || !"PROCESSING".equals(task.get().getStatus())) {
                    return 0;
                }
                task.get().setStatus("SUCCESS");
                return 1;
            });
            when(journalMapper.insert(any(SettlementJournalPO.class))).thenAnswer(invocation -> {
                journals.add(invocation.getArgument(0));
                return 1;
            });
            when(journalMapper.update(any(SettlementJournalPO.class), any())).thenReturn(1);
            when(journalEntryMapper.insert(any(SettlementJournalEntryPO.class))).thenAnswer(invocation -> {
                journalEntries.add(invocation.getArgument(0));
                return 1;
            });
            when(settlementOutboxMapper.insert(any(SettlementEventOutboxPO.class))).thenAnswer(invocation -> {
                settlementOutboxes.add(invocation.getArgument(0));
                return 1;
            });
        }

        /** 原子模拟账户冻结或解冻，并拒绝余额不足。 */
        private int changeFrozen(Long userId, String asset, BigDecimal amount, boolean freeze) {
            AccountBalancePO balance = balance(userId, asset);
            BigDecimal availableChange = freeze ? amount.negate() : amount;
            BigDecimal frozenChange = freeze ? amount : amount.negate();
            return applyChange(userId, asset, availableChange, frozenChange);
        }

        /** 原子模拟清算余额变化，并保存最近更新账户供流水快照读取。 */
        private int applyChange(Long userId, String asset, BigDecimal availableChange, BigDecimal frozenChange) {
            AccountBalancePO balance = balance(userId, asset);
            BigDecimal available = balance.getAvailable().add(availableChange);
            BigDecimal frozen = balance.getFrozen().add(frozenChange);
            if (available.signum() < 0 || frozen.signum() < 0) {
                return 0;
            }
            balance.setAvailable(available);
            balance.setFrozen(frozen);
            lastChangedAccount.set(new AccountKey(userId, asset));
            return 1;
        }

        /** 创建并保存初始余额。 */
        private void putBalance(long userId, String asset, String available, String frozen) {
            AccountBalancePO balance = new AccountBalancePO();
            balance.setId((long) balances.size() + 1);
            balance.setUserId(userId);
            balance.setAsset(asset);
            balance.setAvailable(new BigDecimal(available));
            balance.setFrozen(new BigDecimal(frozen));
            balance.setVersion(0L);
            balances.put(new AccountKey(userId, asset), balance);
        }

        /** 查询指定账户余额。 */
        private AccountBalancePO balance(long userId, String asset) {
            return balances.get(new AccountKey(userId, asset));
        }

        /** 生成用于重复消费前后比较的稳定余额快照。 */
        private List<String> balanceSnapshot() {
            return balances.entrySet().stream().sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + "=" + entry.getValue().getAvailable()
                            + "/" + entry.getValue().getFrozen()).toList();
        }

        /** 复制账户对象，避免生产服务的锁内快照与内存事实对象共享引用。 */
        private static AccountBalancePO copy(AccountBalancePO source) {
            AccountBalancePO target = new AccountBalancePO();
            target.setId(source.getId());
            target.setUserId(source.getUserId());
            target.setAsset(source.getAsset());
            target.setAvailable(source.getAvailable());
            target.setFrozen(source.getFrozen());
            target.setVersion(source.getVersion());
            return target;
        }
    }

    /** 余额账户唯一键。 */
    private record AccountKey(Long userId, String asset) implements Comparable<AccountKey> {
        @Override
        public int compareTo(AccountKey other) {
            int userComparison = userId.compareTo(other.userId);
            return userComparison != 0 ? userComparison : asset.compareTo(other.asset);
        }
    }
}
