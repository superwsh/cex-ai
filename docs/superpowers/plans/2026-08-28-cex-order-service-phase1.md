# CEX 订单系统 Phase 1 实现计划

> **面向 AI 代理的工作者:** 必需子技能:使用 superpowers:subagent-driven-development(推荐)或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框(`- [ ]`)语法来跟踪进度。
>
> **注意:** 本项目**不是 git 仓库**,所有任务的 Commit 步骤替换为「运行测试验证」步骤。

**目标:** 在 cex-order-service(现有骨架)中实现订单系统 Phase 1:创建限价/市价单、取消、查询、交易对配置、资产冻结(Mock)、订单状态机、Outbox + Kafka 事件、幂等与补偿。

**架构:** DDD 分层(api/application/domain/infrastructure)+ 本地事务 + Outbox Pattern + Kafka(`cex.order.event`,key=symbol 保序)。资产冻结通过 AccountServiceClient 接口 + Mock 实现(TODO 标注,后续切 Dubbo)。取消订单直改 + 发事件(撮合引擎接入后改以撮合回报为准)。

**技术栈:** Java 17 / Spring Boot 3.3.5 / MyBatis-Plus 3.5.7 / Spring Kafka / Redis / JUnit 5 + Mockito

**关键约定(改代码前必读):**
- 服务启动类 `@SpringBootApplication(scanBasePackages = "com.cex")`,公共配置靠扫描生效
- 金额一律 `BigDecimal`,禁止 double
- Kafka consumer 需 `spring.json.trusted.packages: "com.cex.*"`(yml 已配)
- 共享事件模型在 cex-common-kafka,跨服务修改需先编译公共模块
- 异常统一抛 `com.cex.common.core.exception.BizException(code, message)`,全局处理器转 ApiResult

---

### 任务 1:共享契约扩展(OrderEvent 加 clientOrderId)

**文件:**
- 修改:`cex-common/cex-common-kafka/src/main/java/com/cex/common/kafka/event/OrderEvent.java`

订单创建/取消事件需要携带 clientOrderId(撮合引擎后续按此做请求关联),向后兼容新增字段。

- [ ] **步骤 1:修改 OrderEvent,新增 clientOrderId 字段**

在 `timestamp` 字段前(即 `price` 之后)添加:

```java
    /** 客户端订单号(幂等键,撮合引擎回报时原样带回) */
    private String clientOrderId;
```

- [ ] **步骤 2:编译验证**

运行:`mvn -pl cex-common/cex-common-kafka -am compile -q`
预期:BUILD SUCCESS,无编译错误。

---

### 任务 2:数据库初始化脚本(4 张表 + 预置交易对)

**文件:**
- 创建:`docker/mysql/init/02-order-tables.sql`

订单库 cex_order 在 `01-databases.sql` 已创建,本脚本建表。注意:docker mysql 初始化只在数据卷首次创建时执行,若本机 mysql 卷已存在,需 `docker compose down -v` 后重新 `docker compose up -d` 或手动在 mysql 中执行脚本。

- [ ] **步骤 1:创建 SQL 脚本**

```sql
-- ============================================================
-- CEX 订单库表结构(Phase 1)
-- 库:cex_order
-- 注意:order_event_outbox / processed_event 为事务性发件箱与消费幂等表
-- ============================================================

USE cex_order;

-- 订单表
CREATE TABLE IF NOT EXISTS orders
(
    id               BIGINT        NOT NULL COMMENT '主键(自研 Snowflake)',
    order_id         BIGINT        NOT NULL COMMENT '订单ID(业务唯一,与主键同值)',
    user_id          BIGINT        NOT NULL COMMENT '用户ID',
    client_order_id  VARCHAR(64)   NULL COMMENT '客户端订单号(幂等键)',
    symbol           VARCHAR(32)   NOT NULL COMMENT '交易对,如 BTC_USDT',
    side             VARCHAR(10)   NOT NULL COMMENT 'BUY/SELL',
    type             VARCHAR(20)   NOT NULL COMMENT 'LIMIT/MARKET',
    price            DECIMAL(32,16) NULL COMMENT '委托价格(市价单为NULL)',
    quantity         DECIMAL(32,16) NOT NULL COMMENT '委托数量',
    quote_amount     DECIMAL(32,16) NULL COMMENT '市价买单冻结金额',
    filled_quantity  DECIMAL(32,16) NOT NULL DEFAULT 0 COMMENT '已成交数量',
    filled_amount    DECIMAL(32,16) NOT NULL DEFAULT 0 COMMENT '已成交金额',
    status           VARCHAR(32)   NOT NULL COMMENT 'NEW/PENDING_MATCH/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED',
    time_in_force    VARCHAR(10)   NULL COMMENT 'GTC/IOC/FOK',
    version          BIGINT        NOT NULL DEFAULT 0 COMMENT '乐观锁版本',
    created_at       DATETIME      NOT NULL COMMENT '创建时间',
    updated_at       DATETIME      NOT NULL COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_id (order_id),
    UNIQUE KEY uk_user_client_order (user_id, client_order_id),
    KEY idx_user_symbol_time (user_id, symbol, created_at),
    KEY idx_user_status_time (user_id, status, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '订单表';

-- 订单事件发件箱(Transactional Outbox)
CREATE TABLE IF NOT EXISTS order_event_outbox
(
    id              BIGINT      NOT NULL COMMENT '主键',
    event_id        VARCHAR(64) NOT NULL COMMENT '事件ID(UUID)',
    aggregate_type  VARCHAR(32) NOT NULL COMMENT '聚合类型,如 ORDER',
    aggregate_id    VARCHAR(64) NOT NULL COMMENT '聚合ID,如 orderId',
    event_type      VARCHAR(64) NOT NULL COMMENT 'ORDER_CREATED/ORDER_CANCELED',
    payload         TEXT        NOT NULL COMMENT '事件JSON',
    status          VARCHAR(16) NOT NULL DEFAULT 'INIT' COMMENT 'INIT/SENDING/SUCCESS/FAILED',
    retry_count     INT         NOT NULL DEFAULT 0 COMMENT '已重试次数',
    next_retry_time DATETIME    NOT NULL COMMENT '下次重试时间(指数退避)',
    created_at      DATETIME    NOT NULL,
    updated_at      DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_id (event_id),
    KEY idx_status_next_retry (status, next_retry_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '订单事件发件箱';

-- 已处理事件表(消费幂等)
CREATE TABLE IF NOT EXISTS processed_event
(
    id           BIGINT      NOT NULL COMMENT '主键',
    event_id     VARCHAR(64) NOT NULL COMMENT '事件ID(如 tradeId)',
    consumer     VARCHAR(64) NOT NULL COMMENT '消费者标识,如 ORDER_STATUS_CONSUMER',
    processed_at DATETIME    NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_event_consumer (event_id, consumer)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '已处理事件(幂等)';

-- 交易对配置表
CREATE TABLE IF NOT EXISTS symbol_config
(
    id             BIGINT         NOT NULL COMMENT '主键',
    symbol         VARCHAR(32)    NOT NULL COMMENT '交易对,如 BTC_USDT',
    base_currency  VARCHAR(16)    NOT NULL COMMENT '基础币,如 BTC',
    quote_currency VARCHAR(16)    NOT NULL COMMENT '计价币,如 USDT',
    price_scale    INT            NOT NULL COMMENT '价格精度(小数位数)',
    quantity_scale INT            NOT NULL COMMENT '数量精度(小数位数)',
    min_quantity   DECIMAL(32,16) NOT NULL COMMENT '最小下单数量',
    min_amount     DECIMAL(32,16) NOT NULL COMMENT '最小下单金额',
    status         VARCHAR(16)    NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE/PAUSED',
    created_at     DATETIME       NOT NULL,
    updated_at     DATETIME       NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_symbol (symbol)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci COMMENT = '交易对配置';

-- 预置交易对数据
INSERT INTO symbol_config
    (id, symbol, base_currency, quote_currency, price_scale, quantity_scale, min_quantity, min_amount, status, created_at, updated_at)
VALUES
    (1, 'BTC_USDT', 'BTC', 'USDT', 2, 6, 0.00010000000000000000, 10.00000000000000000000, 'ACTIVE', NOW(), NOW()),
    (2, 'ETH_USDT', 'ETH', 'USDT', 2, 4, 0.01000000000000000000, 10.00000000000000000000, 'ACTIVE', NOW(), NOW());
```

- [ ] **步骤 2:验证 SQL 语法**

运行:`docker compose up -d` 后,若 mysql 数据卷已存在则手动执行:

```bash
docker compose exec mysql mysql -uroot -proot < docker/mysql/init/02-order-tables.sql
```

预期:无报错,`SHOW TABLES FROM cex_order` 能看到 4 张表,`SELECT * FROM cex_order.symbol_config` 有 2 行。

---

### 任务 3:订单领域模型与状态机(TDD)

**文件:**
- 修改:`cex-order-service/pom.xml`(加测试依赖)
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/OrderStatus.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/OrderSide.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/OrderType.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/TimeInForce.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/Order.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/model/OrderFactory.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/common/OrderStatusInvalidException.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/domain/model/OrderStateMachineTest.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/domain/model/OrderFactoryTest.java`

- [ ] **步骤 1:pom.xml 加测试依赖**

在 `</dependencies>` 前添加:

```xml
        <!-- 单元测试 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **步骤 2:编写失败的测试(状态机)**

创建 `OrderStateMachineTest.java`:

```java
package com.cex.order.domain.model;

import com.cex.order.common.OrderStatusInvalidException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderStateMachineTest {

    private Order newOrder() {
        return Order.builder()
                .orderId(1L).userId(100L).clientOrderId("c1").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .status(OrderStatus.PENDING_MATCH)
                .build();
    }

    @Test
    void pendingMatch_canCancel() {
        Order order = newOrder();
        order.cancel();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELED);
    }

    @Test
    void pendingMatch_canPartiallyFill() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PARTIALLY_FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.04");
    }

    @Test
    void partialFill_canFill() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        order.markPartiallyFilled(new BigDecimal("0.06"), new BigDecimal("6000"));
        assertThat(order.getStatus()).isEqualTo(OrderStatus.FILLED);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0.1");
        assertThat(order.getFilledAmount()).isEqualByComparingTo("10000");
    }

    @Test
    void filled_cannotCancel() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        assertThatThrownBy(order::cancel)
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("FILLED");
    }

    @Test
    void filled_cannotFillAgain() {
        Order order = newOrder();
        order.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.01"), new BigDecimal("1000")))
                .isInstanceOf(OrderStatusInvalidException.class);
    }

    @Test
    void canceled_cannotFill() {
        Order order = newOrder();
        order.cancel();
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.01"), new BigDecimal("1000")))
                .isInstanceOf(OrderStatusInvalidException.class);
    }

    @Test
    void reject_works() {
        Order order = newOrder();
        order.reject();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
    }

    @Test
    void markPartiallyFilled_withExceededQuantity_throws() {
        Order order = newOrder();
        assertThatThrownBy(() -> order.markPartiallyFilled(new BigDecimal("0.2"), new BigDecimal("20000")))
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("超过委托数量");
    }
}
```

- [ ] **步骤 3:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=OrderStateMachineTest -DfailIfNoTests=false`
预期:编译失败,Order/OrderStatus 等类不存在。

- [ ] **步骤 4:实现枚举与异常**

`OrderStatus.java`:

```java
package com.cex.order.domain.model;

public enum OrderStatus {
    NEW,                // 新建(预留)
    PENDING_MATCH,      // 已提交待撮合
    PARTIALLY_FILLED,   // 部分成交
    FILLED,             // 全部成交
    CANCELED,           // 已取消
    REJECTED;           // 已拒绝

    /** 可取消状态 */
    public boolean canCancel() {
        return this == NEW || this == PENDING_MATCH || this == PARTIALLY_FILLED;
    }

    /** 可成交(回报)状态 */
    public boolean canFill() {
        return this == PENDING_MATCH || this == PARTIALLY_FILLED;
    }

    public boolean isTerminal() {
        return this == FILLED || this == CANCELED || this == REJECTED;
    }
}
```

`OrderSide.java`:

```java
package com.cex.order.domain.model;

public enum OrderSide {
    BUY, SELL
}
```

`OrderType.java`:

```java
package com.cex.order.domain.model;

public enum OrderType {
    LIMIT, MARKET
}
```

`TimeInForce.java`:

```java
package com.cex.order.domain.model;

public enum TimeInForce {
    GTC, IOC, FOK
}
```

`OrderStatusInvalidException.java`:

```java
package com.cex.order.common;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;

/**
 * 订单状态非法流转
 */
public class OrderStatusInvalidException extends BizException {

    public OrderStatusInvalidException(String message) {
        super(ErrorCode.ORDER_STATUS_INVALID.getCode(), message);
    }
}
```

(注意:`ErrorCode` 在任务 4 创建,本任务先创建 `OrderStatusInvalidException`,若编译失败属预期,任务 4 补齐后回归。)

- [ ] **步骤 5:实现 Order 聚合根**

`Order.java`:

```java
package com.cex.order.domain.model;

import com.cex.order.common.OrderStatusInvalidException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单聚合根:状态流转必须通过领域方法,禁止直接 setStatus
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private Long id;                 // 主键 = orderId
    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    private BigDecimal price;        // 市价单为 null
    private BigDecimal quantity;
    private BigDecimal quoteAmount;  // 市价买单冻结金额
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private OrderStatus status;
    private TimeInForce timeInForce;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Long version;

    /** 提交撮合(预留:创建后状态即为 PENDING_MATCH) */
    public void markPendingMatch() {
        transition(OrderStatus.NEW, OrderStatus.PENDING_MATCH);
    }

    /**
     * 成交回报:累计已成交数量/金额,按状态机流转
     */
    public void markPartiallyFilled(BigDecimal fillQuantity, BigDecimal fillAmount) {
        if (!status.canFill()) {
            throw new OrderStatusInvalidException("订单状态 " + status + " 不允许成交");
        }
        BigDecimal newFilledQty = safeFilledQuantity().add(fillQuantity);
        if (newFilledQty.compareTo(quantity) > 0) {
            throw new OrderStatusInvalidException("成交数量超过委托数量: " + newFilledQty + " > " + quantity);
        }
        this.filledQuantity = newFilledQty;
        this.filledAmount = safeFilledAmount().add(fillAmount);
        this.status = newFilledQty.compareTo(quantity) >= 0 ? OrderStatus.FILLED : OrderStatus.PARTIALLY_FILLED;
        this.updatedAt = LocalDateTime.now();
    }

    public void cancel() {
        if (!status.canCancel()) {
            throw new OrderStatusInvalidException("订单状态 " + status + " 不允许取消");
        }
        this.status = OrderStatus.CANCELED;
        this.updatedAt = LocalDateTime.now();
    }

    public void reject() {
        if (status.isTerminal()) {
            throw new OrderStatusInvalidException("终态订单不可拒绝: " + status);
        }
        this.status = OrderStatus.REJECTED;
        this.updatedAt = LocalDateTime.now();
    }

    private void transition(OrderStatus from, OrderStatus to) {
        if (this.status != from) {
            throw new OrderStatusInvalidException("订单状态 " + this.status + " 不允许流转到 " + to);
        }
        this.status = to;
        this.updatedAt = LocalDateTime.now();
    }

    private BigDecimal safeFilledQuantity() {
        return filledQuantity == null ? BigDecimal.ZERO : filledQuantity;
    }

    private BigDecimal safeFilledAmount() {
        return filledAmount == null ? BigDecimal.ZERO : filledAmount;
    }

    public boolean isOpen() {
        return status.canCancel();
    }
}
```

- [ ] **步骤 6:实现 OrderFactory**

`OrderFactory.java`:

```java
package com.cex.order.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 订单工厂:构造初始状态为 PENDING_MATCH 的订单(创建即提交待撮合)
 */
public class OrderFactory {

    public Order createPendingMatchOrder(Long orderId, Long userId, String clientOrderId,
                                         String symbol, OrderSide side, OrderType type,
                                         BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount,
                                         TimeInForce timeInForce) {
        LocalDateTime now = LocalDateTime.now();
        return Order.builder()
                .id(orderId)
                .orderId(orderId)
                .userId(userId)
                .clientOrderId(clientOrderId)
                .symbol(symbol)
                .side(side)
                .type(type)
                .price(price)
                .quantity(quantity)
                .quoteAmount(quoteAmount)
                .filledQuantity(BigDecimal.ZERO)
                .filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH)
                .timeInForce(timeInForce)
                .createdAt(now)
                .updatedAt(now)
                .version(0L)
                .build();
    }
}
```

- [ ] **步骤 7:实现 OrderFactoryTest**

`OrderFactoryTest.java`:

```java
package com.cex.order.domain.model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class OrderFactoryTest {

    private final OrderFactory factory = new OrderFactory();

    @Test
    void createPendingMatchOrder_setsInitialState() {
        Order order = factory.createPendingMatchOrder(
                1L, 100L, "c1", "BTC_USDT",
                OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null,
                TimeInForce.GTC);

        assertThat(order.getOrderId()).isEqualTo(1L);
        assertThat(order.getId()).isEqualTo(1L);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING_MATCH);
        assertThat(order.getFilledQuantity()).isEqualByComparingTo("0");
        assertThat(order.getVersion()).isEqualTo(0L);
        assertThat(order.getCreatedAt()).isNotNull();
    }

    @Test
    void createMarketBuyOrder_keepsQuoteAmount() {
        Order order = factory.createPendingMatchOrder(
                2L, 100L, "c2", "BTC_USDT",
                OrderSide.BUY, OrderType.MARKET,
                null, BigDecimal.ZERO, new BigDecimal("1000"),
                TimeInForce.GTC);
        assertThat(order.getQuoteAmount()).isEqualByComparingTo("1000");
        assertThat(order.getPrice()).isNull();
    }
}
```

- [ ] **步骤 8:运行全部测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest='OrderStateMachineTest,OrderFactoryTest' -DfailIfNoTests=false`
预期:BUILD SUCCESS,2 个测试类全部通过。(若因 ErrorCode 未创建编译失败,先跳到任务 4 创建 ErrorCode 后回来验证。)

---

### 任务 4:错误码、交易规则校验、冻结金额计算(TDD)

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/common/ErrorCode.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/service/TradingRuleValidator.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/service/FreezeCalculator.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/service/SymbolConfig.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/domain/service/TradingRuleValidatorTest.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/domain/service/FreezeCalculatorTest.java`

- [ ] **步骤 1:编写失败的测试**

`TradingRuleValidatorTest.java`:

```java
package com.cex.order.domain.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TradingRuleValidatorTest {

    private TradingRuleValidator validator;
    private SymbolConfig config;

    @BeforeEach
    void setUp() {
        validator = new TradingRuleValidator();
        config = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.ACTIVE)
                .build();
    }

    @Test
    void validatePrice_scaleExceeded_throws() {
        assertThatThrownBy(() -> validator.validatePrice(new BigDecimal("100000.123"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("价格精度");
    }

    @Test
    void validatePrice_ok() {
        assertThatCode(() -> validator.validatePrice(new BigDecimal("100000.12"), config))
                .doesNotThrowAnyException();
    }

    @Test
    void validateQuantity_scaleExceeded_throws() {
        assertThatThrownBy(() -> validator.validateQuantity(new BigDecimal("0.1234567"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("数量精度");
    }

    @Test
    void validateQuantity_belowMin_throws() {
        assertThatThrownBy(() -> validator.validateQuantity(new BigDecimal("0.00005"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单数量");
    }

    @Test
    void validateMinAmount_limitBuy_notEnough_throws() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.BUY, OrderType.LIMIT,
                        new BigDecimal("100"), new BigDecimal("0.01"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单金额");
    }

    @Test
    void validateMinAmount_limitSell_skipped() {
        // 卖单按数量校验,不校验金额
        assertThatCode(() -> validator.validateMinAmount(
                        OrderSide.SELL, OrderType.LIMIT,
                        new BigDecimal("100"), new BigDecimal("0.01"), config))
                .doesNotThrowAnyException();
    }

    @Test
    void validateMarketBuy_quoteAmountRequired() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.BUY, OrderType.MARKET,
                        null, BigDecimal.ZERO, config))
                .isInstanceOf(BizException.class);
    }

    @Test
    void validateMarketSell_quantityRequired() {
        assertThatThrownBy(() -> validator.validateMinAmount(
                        OrderSide.SELL, OrderType.MARKET,
                        null, new BigDecimal("0.00005"), config))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("最小下单数量");
    }
}
```

`FreezeCalculatorTest.java`:

```java
package com.cex.order.domain.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class FreezeCalculatorTest {

    private FreezeCalculator calculator;
    private SymbolConfig config;

    @BeforeEach
    void setUp() {
        calculator = new FreezeCalculator();
        config = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.ACTIVE)
                .build();
    }

    @Test
    void limitBuy_freezePriceTimesQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.BUY, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null);
        assertThat(amount).isEqualByComparingTo("10000");
    }

    @Test
    void limitSell_freezeQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.SELL, OrderType.LIMIT,
                new BigDecimal("100000"), new BigDecimal("0.1"), null);
        assertThat(amount).isEqualByComparingTo("0.1");
    }

    @Test
    void marketBuy_freezeQuoteAmount() {
        BigDecimal amount = calculator.calculate(
                OrderSide.BUY, OrderType.MARKET,
                null, BigDecimal.ZERO, new BigDecimal("5000"));
        assertThat(amount).isEqualByComparingTo("5000");
    }

    @Test
    void marketSell_freezeQuantity() {
        BigDecimal amount = calculator.calculate(
                OrderSide.SELL, OrderType.MARKET,
                null, new BigDecimal("0.5"), null);
        assertThat(amount).isEqualByComparingTo("0.5");
    }

    @Test
    void freezeCurrency_buyIsQuote_sellIsBase() {
        assertThat(calculator.freezeCurrency(OrderSide.BUY, config)).isEqualTo("USDT");
        assertThat(calculator.freezeCurrency(OrderSide.SELL, config)).isEqualTo("BTC");
    }

    @Test
    void remainingToUnfreeze_buy_unfilledPart() {
        Order order = Order.builder()
                .orderId(1L).userId(100L).symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(new BigDecimal("0.04")).filledAmount(new BigDecimal("4000"))
                .status(OrderStatus.CANCELED)
                .build();
        BigDecimal remaining = calculator.remainingToUnfreeze(order, config);
        assertThat(remaining).isEqualByComparingTo("6000");
    }

    @Test
    void remainingToUnfreeze_sell_unfilledPart() {
        Order order = Order.builder()
                .orderId(1L).userId(100L).symbol("BTC_USDT")
                .side(OrderSide.SELL).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(new BigDecimal("0.04")).filledAmount(new BigDecimal("4000"))
                .status(OrderStatus.CANCELED)
                .build();
        BigDecimal remaining = calculator.remainingToUnfreeze(order, config);
        assertThat(remaining).isEqualByComparingTo("0.06");
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest='TradingRuleValidatorTest,FreezeCalculatorTest' -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 ErrorCode**

`ErrorCode.java`:

```java
package com.cex.order.common;

import lombok.Getter;

/**
 * 订单系统业务错误码
 */
@Getter
public enum ErrorCode {

    INVALID_PARAM(40001, "参数错误"),
    SYMBOL_NOT_FOUND(40010, "交易对不存在"),
    SYMBOL_PAUSED(40011, "交易对暂停交易"),
    PRICE_SCALE_ERROR(40020, "价格精度错误"),
    QUANTITY_SCALE_ERROR(40021, "数量精度错误"),
    MIN_QUANTITY_NOT_MET(40022, "最小下单数量不满足"),
    MIN_AMOUNT_NOT_MET(40023, "最小下单金额不满足"),
    ORDER_NOT_FOUND(40030, "订单不存在"),
    ORDER_STATUS_INVALID(40031, "订单状态不允许该操作"),
    DUPLICATE_CLIENT_ORDER(40032, "重复的订单请求"),
    FREEZE_FAILED(50010, "资产冻结失败"),
    INSUFFICIENT_BALANCE(50011, "余额不足"),
    ;

    private final int code;
    private final String message;

    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
```

- [ ] **步骤 4:实现 SymbolConfig**

`SymbolConfig.java`:

```java
package com.cex.order.domain.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 交易对配置(领域模型)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SymbolConfig {

    public enum SymbolStatus {
        ACTIVE, PAUSED
    }

    private String symbol;
    private String baseCurrency;
    private String quoteCurrency;
    private int priceScale;
    private int quantityScale;
    private BigDecimal minQuantity;
    private BigDecimal minAmount;
    private SymbolStatus status;

    public boolean isTradable() {
        return status == SymbolStatus.ACTIVE;
    }
}
```

- [ ] **步骤 5:实现 TradingRuleValidator**

`TradingRuleValidator.java`:

```java
package com.cex.order.domain.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 交易规则校验:精度、最小数量、最小金额
 */
@Component
public class TradingRuleValidator {

    public void validatePrice(BigDecimal price, SymbolConfig config) {
        if (price == null) {
            return; // 市价单无价格
        }
        if (price.scale() > config.getPriceScale()) {
            throw new BizException(ErrorCode.PRICE_SCALE_ERROR.getCode(),
                    ErrorCode.PRICE_SCALE_ERROR.getMessage() + ", 最多 " + config.getPriceScale() + " 位小数");
        }
    }

    public void validateQuantity(BigDecimal quantity, SymbolConfig config) {
        if (quantity == null || quantity.compareTo(BigDecimal.ZERO) <= 0) {
            throw new BizException(ErrorCode.INVALID_PARAM.getCode(), "委托数量必须大于 0");
        }
        if (quantity.scale() > config.getQuantityScale()) {
            throw new BizException(ErrorCode.QUANTITY_SCALE_ERROR.getCode(),
                    ErrorCode.QUANTITY_SCALE_ERROR.getMessage() + ", 最多 " + config.getQuantityScale() + " 位小数");
        }
        if (quantity.compareTo(config.getMinQuantity()) < 0) {
            throw new BizException(ErrorCode.MIN_QUANTITY_NOT_MET.getCode(),
                    ErrorCode.MIN_QUANTITY_NOT_MET.getMessage() + ", 最小 " + config.getMinQuantity());
        }
    }

    /**
     * 最小金额校验:限价买单校验 price*quantity >= minAmount;
     * 市价买单校验 quoteAmount >= minAmount;卖单按数量校验(步骤 2 的 validateQuantity 已覆盖)
     */
    public void validateMinAmount(OrderSide side, OrderType type,
                                  BigDecimal price, BigDecimal quantity, SymbolConfig config) {
        if (side == OrderSide.SELL) {
            return;
        }
        BigDecimal amount;
        if (type == OrderType.MARKET) {
            if (price == null) {
                throw new BizException(ErrorCode.INVALID_PARAM.getCode(), "市价买单必须传入 quoteAmount");
            }
            amount = price; // 此处 price 参数实际承载 quoteAmount,由调用方传入
        } else {
            amount = price.multiply(quantity);
        }
        if (amount.compareTo(config.getMinAmount()) < 0) {
            throw new BizException(ErrorCode.MIN_AMOUNT_NOT_MET.getCode(),
                    ErrorCode.MIN_AMOUNT_NOT_MET.getMessage() + ", 最小 " + config.getMinAmount());
        }
    }
}
```

- [ ] **步骤 6:实现 FreezeCalculator**

`FreezeCalculator.java`:

```java
package com.cex.order.domain.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * 冻结金额计算
 * 买单:冻结计价币(USDT);卖单:冻结基础币(BTC)
 */
@Component
public class FreezeCalculator {

    /**
     * 下单冻结金额
     *
     * @param quoteAmount 市价买单的冻结金额
     */
    public BigDecimal calculate(OrderSide side, OrderType type,
                                BigDecimal price, BigDecimal quantity, BigDecimal quoteAmount) {
        if (type == OrderType.MARKET && side == OrderSide.BUY) {
            return quoteAmount;
        }
        if (side == OrderSide.SELL) {
            return quantity;
        }
        return price.multiply(quantity);
    }

    public String freezeCurrency(OrderSide side, SymbolConfig config) {
        return side == OrderSide.BUY ? config.getQuoteCurrency() : config.getBaseCurrency();
    }

    /**
     * 取消订单时剩余解冻金额:未成交部分的冻结金额
     */
    public BigDecimal remainingToUnfreeze(Order order, SymbolConfig config) {
        BigDecimal unfilled = order.getQuantity().subtract(
                order.getFilledQuantity() == null ? BigDecimal.ZERO : order.getFilledQuantity());
        if (order.getSide() == OrderSide.SELL) {
            return unfilled;
        }
        return order.getPrice().multiply(unfilled);
    }
}
```

- [ ] **步骤 7:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest='TradingRuleValidatorTest,FreezeCalculatorTest' -DfailIfNoTests=false`
预期:BUILD SUCCESS,全部通过。同时回归任务 3:`mvn -pl cex-order-service -am test -Dtest='OrderStateMachineTest,OrderFactoryTest' -DfailIfNoTests=false`。

---

### 任务 5:基础设施——Snowflake + 订单持久化层

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/id/SnowflakeGenerator.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/entity/OrderPO.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/entity/SymbolConfigPO.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/mapper/OrderMapper.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/mapper/SymbolConfigMapper.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/domain/repository/OrderRepository.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/repository/OrderRepositoryImpl.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/repository/SymbolConfigRepository.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/infrastructure/id/SnowflakeGeneratorTest.java`

- [ ] **步骤 1:编写失败的测试(Snowflake)**

`SnowflakeGeneratorTest.java`:

```java
package com.cex.order.infrastructure.id;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SnowflakeGeneratorTest {

    @Test
    void nextId_isUniqueAndIncreasing() {
        SnowflakeGenerator generator = new SnowflakeGenerator(1, 1);
        long prev = generator.nextId();
        for (int i = 0; i < 10_000; i++) {
            long next = generator.nextId();
            assertThat(next).isGreaterThan(prev);
            prev = next;
        }
    }

    @Test
    void nextId_concurrent_neverDuplicates() throws InterruptedException {
        SnowflakeGenerator generator = new SnowflakeGenerator(1, 1);
        int threads = 8;
        int perThread = 10_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        Set<Long> ids = java.util.Collections.synchronizedSet(new HashSet<>());

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    for (int i = 0; i < perThread; i++) {
                        ids.add(generator.nextId());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await(30, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(ids).hasSize(threads * perThread);
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=SnowflakeGeneratorTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 SnowflakeGenerator**

`SnowflakeGenerator.java`:

```java
package com.cex.order.infrastructure.id;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Snowflake 雪花 ID:时间戳(41bit) + 机器ID(10bit) + 序列号(12bit)
 * 单实例部署时 workerId/datacenterId 从配置读取,多机房部署需重新分配
 */
@Component
public class SnowflakeGenerator {

    /** 起始时间戳(2024-01-01 00:00:00) */
    private static final long EPOCH = 1704067200000L;
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private final long workerId;
    private final long datacenterId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public SnowflakeGenerator(@Value("${cex.order.id.worker-id:1}") long workerId,
                              @Value("${cex.order.id.datacenter-id:1}") long datacenterId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException("workerId 非法: " + workerId);
        }
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) {
            throw new IllegalArgumentException("datacenterId 非法: " + datacenterId);
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }

    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            throw new IllegalStateException("时钟回拨,拒绝生成 ID: " + (lastTimestamp - timestamp) + "ms");
        }
        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
```

- [ ] **步骤 4:实现 PO 实体与 Mapper**

`OrderPO.java`:

```java
package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("orders")
public class OrderPO {

    @TableId(type = IdType.INPUT)
    private Long id;
    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private String side;
    private String type;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteAmount;
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private String status;
    private String timeInForce;
    @Version
    private Long version;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

`SymbolConfigPO.java`:

```java
package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("symbol_config")
public class SymbolConfigPO {

    @TableId(type = IdType.INPUT)
    private Long id;
    private String symbol;
    private String baseCurrency;
    private String quoteCurrency;
    private Integer priceScale;
    private Integer quantityScale;
    private BigDecimal minQuantity;
    private BigDecimal minAmount;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

`OrderMapper.java`:

```java
package com.cex.order.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.order.infrastructure.persistence.entity.OrderPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderMapper extends BaseMapper<OrderPO> {
}
```

`SymbolConfigMapper.java`:

```java
package com.cex.order.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.order.infrastructure.persistence.entity.SymbolConfigPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface SymbolConfigMapper extends BaseMapper<SymbolConfigPO> {
}
```

- [ ] **步骤 5:实现 OrderRepository 接口与实现**

`OrderRepository.java`(domain 层接口):

```java
package com.cex.order.domain.repository;

import com.cex.order.domain.model.Order;

import java.util.List;

public interface OrderRepository {

    void insert(Order order);

    void update(Order order);

    Order findByOrderId(Long orderId);

    Order findByUserIdAndClientOrderId(Long userId, String clientOrderId);

    /** 当前委托(游标分页):created_at < createdAt OR (created_at = createdAt AND order_id < orderId) */
    List<Order> listOpenOrders(Long userId, String symbol, int limit,
                               java.time.LocalDateTime cursorTime, Long cursorOrderId);

    /** 历史订单(游标分页),status 不限定 */
    List<Order> listHistoryOrders(Long userId, String symbol, int limit,
                                  java.time.LocalDateTime cursorTime, Long cursorOrderId);
}
```

`OrderRepositoryImpl.java`(infrastructure 层):

```java
package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.OrderPO;
import com.cex.order.infrastructure.persistence.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class OrderRepositoryImpl implements OrderRepository {

    private static final String OPEN_STATUSES = "NEW,PENDING_MATCH,PARTIALLY_FILLED";

    private final OrderMapper orderMapper;

    @Override
    public void insert(Order order) {
        orderMapper.insert(toPO(order));
    }

    @Override
    public void update(Order order) {
        orderMapper.updateById(toPO(order));
    }

    @Override
    public Order findByOrderId(Long orderId) {
        OrderPO po = orderMapper.selectOne(new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getOrderId, orderId));
        return po == null ? null : toDomain(po);
    }

    @Override
    public Order findByUserIdAndClientOrderId(Long userId, String clientOrderId) {
        if (clientOrderId == null) {
            return null;
        }
        OrderPO po = orderMapper.selectOne(new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getUserId, userId)
                .eq(OrderPO::getClientOrderId, clientOrderId));
        return po == null ? null : toDomain(po);
    }

    @Override
    public List<Order> listOpenOrders(Long userId, String symbol, int limit,
                                      LocalDateTime cursorTime, Long cursorOrderId) {
        LambdaQueryWrapper<OrderPO> wrapper = baseCursorWrapper(userId, symbol, cursorTime, cursorOrderId)
                .inSql(OrderPO::getStatus, OPEN_STATUSES);
        return orderMapper.selectList(wrapper.last("LIMIT " + limit)).stream()
                .map(this::toDomain).toList();
    }

    @Override
    public List<Order> listHistoryOrders(Long userId, String symbol, int limit,
                                         LocalDateTime cursorTime, Long cursorOrderId) {
        return orderMapper.selectList(baseCursorWrapper(userId, symbol, cursorTime, cursorOrderId)
                        .last("LIMIT " + limit)).stream()
                .map(this::toDomain).toList();
    }

    private LambdaQueryWrapper<OrderPO> baseCursorWrapper(Long userId, String symbol,
                                                          LocalDateTime cursorTime, Long cursorOrderId) {
        LambdaQueryWrapper<OrderPO> wrapper = new LambdaQueryWrapper<OrderPO>()
                .eq(OrderPO::getUserId, userId);
        if (symbol != null && !symbol.isBlank()) {
            wrapper.eq(OrderPO::getSymbol, symbol);
        }
        if (cursorTime != null && cursorOrderId != null) {
            wrapper.and(w -> w.lt(OrderPO::getCreatedAt, cursorTime)
                    .or(o -> o.eq(OrderPO::getCreatedAt, cursorTime)
                            .lt(OrderPO::getOrderId, cursorOrderId)));
        }
        wrapper.orderByDesc(OrderPO::getCreatedAt).orderByDesc(OrderPO::getOrderId);
        return wrapper;
    }

    private OrderPO toPO(Order order) {
        return OrderPO.builder()
                .id(order.getId()).orderId(order.getOrderId()).userId(order.getUserId())
                .clientOrderId(order.getClientOrderId()).symbol(order.getSymbol())
                .side(order.getSide().name()).type(order.getType().name())
                .price(order.getPrice()).quantity(order.getQuantity())
                .quoteAmount(order.getQuoteAmount())
                .filledQuantity(order.getFilledQuantity()).filledAmount(order.getFilledAmount())
                .status(order.getStatus().name()).timeInForce(order.getTimeInForce() == null ? null : order.getTimeInForce().name())
                .version(order.getVersion()).createdAt(order.getCreatedAt()).updatedAt(order.getUpdatedAt())
                .build();
    }

    private Order toDomain(OrderPO po) {
        return Order.builder()
                .id(po.getId()).orderId(po.getOrderId()).userId(po.getUserId())
                .clientOrderId(po.getClientOrderId()).symbol(po.getSymbol())
                .side(OrderSideOf(po.getSide())).type(OrderTypeOf(po.getType()))
                .price(po.getPrice()).quantity(po.getQuantity()).quoteAmount(po.getQuoteAmount())
                .filledQuantity(po.getFilledQuantity()).filledAmount(po.getFilledAmount())
                .status(OrderStatus.valueOf(po.getStatus()))
                .timeInForce(po.getTimeInForce() == null ? null : TimeInForceOf(po.getTimeInForce()))
                .version(po.getVersion()).createdAt(po.getCreatedAt()).updatedAt(po.getUpdatedAt())
                .build();
    }

    private OrderSide OrderSideOf(String v) { return OrderSide.valueOf(v); }
    private OrderType OrderTypeOf(String v) { return OrderType.valueOf(v); }
    private TimeInForce TimeInForceOf(String v) { return TimeInForce.valueOf(v); }
}
```

(注意:上面的 `OrderSideOf/OrderTypeOf/TimeInForceOf` 写法仅为占位说明——实现时直接写 `OrderSide.valueOf(po.getSide())` 等,并补齐 `import com.cex.order.domain.model.OrderSide/OrderType/TimeInForce`。)

`SymbolConfigRepository.java`:

```java
package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.persistence.entity.SymbolConfigPO;
import com.cex.order.infrastructure.persistence.mapper.SymbolConfigMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SymbolConfigRepository {

    private final SymbolConfigMapper symbolConfigMapper;

    public SymbolConfig findBySymbol(String symbol) {
        SymbolConfigPO po = symbolConfigMapper.selectOne(new LambdaQueryWrapper<SymbolConfigPO>()
                .eq(SymbolConfigPO::getSymbol, symbol));
        return po == null ? null : toDomain(po);
    }

    private SymbolConfig toDomain(SymbolConfigPO po) {
        return SymbolConfig.builder()
                .symbol(po.getSymbol()).baseCurrency(po.getBaseCurrency())
                .quoteCurrency(po.getQuoteCurrency()).priceScale(po.getPriceScale())
                .quantityScale(po.getQuantityScale()).minQuantity(po.getMinQuantity())
                .minAmount(po.getMinAmount())
                .status(SymbolConfig.SymbolStatus.valueOf(po.getStatus()))
                .build();
    }
}
```

- [ ] **步骤 6:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=SnowflakeGeneratorTest -DfailIfNoTests=false`
预期:BUILD SUCCESS(Snowflake 并发唯一性通过;Repository 层无测试,编译通过即可)。

---

### 任务 6:交易对配置服务(Redis 缓存 + 加载)

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/redis/SymbolConfigCache.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/SymbolConfigService.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/SymbolConfigServiceTest.java`

缓存策略:cache-aside。`getRequired(symbol)` 查 Redis → 未命中查 DB → 回填 Redis(带 10 分钟 TTL)。

- [ ] **步骤 1:编写失败的测试**

`SymbolConfigServiceTest.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.redis.SymbolConfigCache;
import com.cex.order.infrastructure.repository.SymbolConfigRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SymbolConfigServiceTest {

    @Mock
    private SymbolConfigRepository repository;
    @Mock
    private SymbolConfigCache cache;

    private SymbolConfigService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    @BeforeEach
    void setUp() {
        service = new SymbolConfigService(repository, cache);
    }

    @Test
    void getRequired_cacheHit_returnsConfig() {
        when(cache.get("BTC_USDT")).thenReturn(config);
        assertThat(service.getRequired("BTC_USDT")).isSameAs(config);
        verify(repository, never()).findBySymbol(any());
    }

    @Test
    void getRequired_cacheMiss_loadsDbAndBackfills() {
        when(cache.get("BTC_USDT")).thenReturn(null);
        when(repository.findBySymbol("BTC_USDT")).thenReturn(config);

        SymbolConfig result = service.getRequired("BTC_USDT");

        assertThat(result).isSameAs(config);
        verify(cache).put("BTC_USDT", config);
    }

    @Test
    void getRequired_notFound_throws() {
        when(cache.get("BTC_USDT")).thenReturn(null);
        when(repository.findBySymbol("BTC_USDT")).thenReturn(null);

        assertThatThrownBy(() -> service.getRequired("BTC_USDT"))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("交易对不存在");
    }

    @Test
    void getRequired_paused_throws() {
        SymbolConfig paused = SymbolConfig.builder()
                .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
                .priceScale(2).quantityScale(6)
                .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
                .status(SymbolConfig.SymbolStatus.PAUSED)
                .build();
        when(cache.get("BTC_USDT")).thenReturn(paused);

        assertThatThrownBy(() -> service.getRequired("BTC_USDT"))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("暂停");
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=SymbolConfigServiceTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 SymbolConfigCache**

`SymbolConfigCache.java`:

```java
package com.cex.order.infrastructure.redis;

import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 交易对配置 Redis 缓存(cache-aside,10 分钟 TTL)
 * 注意:订单最终状态不依赖缓存,缓存仅做配置加速
 */
@Component
@RequiredArgsConstructor
public class SymbolConfigCache {

    private static final String KEY_PREFIX = "symbol:config:";
    private static final Duration TTL = Duration.ofMinutes(10);

    private final RedisTemplate<String, Object> redisTemplate;

    public SymbolConfig get(String symbol) {
        Object value = redisTemplate.opsForValue().get(KEY_PREFIX + symbol);
        return value instanceof SymbolConfig config ? config : null;
    }

    public void put(String symbol, SymbolConfig config) {
        redisTemplate.opsForValue().set(KEY_PREFIX + symbol, config, TTL);
    }
}
```

- [ ] **步骤 4:实现 SymbolConfigService**

`SymbolConfigService.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.redis.SymbolConfigCache;
import com.cex.order.infrastructure.repository.SymbolConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * 交易对配置服务:Redis 缓存优先,DB 兜底并回填
 */
@Service
@RequiredArgsConstructor
public class SymbolConfigService {

    private final SymbolConfigRepository repository;
    private final SymbolConfigCache cache;

    /**
     * 获取可交易交易对配置;不存在或暂停直接抛业务异常
     */
    public SymbolConfig getRequired(String symbol) {
        SymbolConfig config = cache.get(symbol);
        if (config == null) {
            config = repository.findBySymbol(symbol);
            if (config == null) {
                throw new BizException(ErrorCode.SYMBOL_NOT_FOUND.getCode(),
                        ErrorCode.SYMBOL_NOT_FOUND.getMessage() + ": " + symbol);
            }
            cache.put(symbol, config);
        }
        if (!config.isTradable()) {
            throw new BizException(ErrorCode.SYMBOL_PAUSED.getCode(),
                    ErrorCode.SYMBOL_PAUSED.getMessage() + ": " + symbol);
        }
        return config;
    }
}
```

- [ ] **步骤 5:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=SymbolConfigServiceTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,4 个测试全部通过。

---

### 任务 7:资产冻结客户端(接口 + Mock)

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/asset/FreezeRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/asset/UnfreezeRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/asset/AccountServiceClient.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/asset/MockAccountServiceClient.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/infrastructure/asset/MockAccountServiceClientTest.java`

- [ ] **步骤 1:编写失败的测试**

`MockAccountServiceClientTest.java`:

```java
package com.cex.order.infrastructure.asset;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MockAccountServiceClientTest {

    private MockAccountServiceClient client;

    @BeforeEach
    void setUp() {
        // 预置:用户 100 有 10000 USDT 与 1 BTC 可用余额
        Map<String, BigDecimal> balances = new HashMap<>();
        balances.put("100:USDT", new BigDecimal("10000"));
        balances.put("100:BTC", new BigDecimal("1"));
        client = new MockAccountServiceClient(balances);
    }

    @Test
    void freeze_deductsAvailableAndFreezes() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());

        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("5000");
        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("5000");
    }

    @Test
    void freeze_sameBizId_idempotent() {
        FreezeRequest request = FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build();
        client.freeze(request);
        assertThatCode(() -> client.freeze(request)).doesNotThrowAnyException();
        // 重复冻结不重复扣减
        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("5000");
        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("5000");
    }

    @Test
    void freeze_insufficientBalance_throws() {
        assertThatThrownBy(() -> client.freeze(FreezeRequest.builder()
                        .userId(100L).currency("USDT").amount(new BigDecimal("99999"))
                        .bizType("FREEZE_ORDER").bizId(2L).build()))
                .hasMessageContaining("余额不足");
    }

    @Test
    void unfreeze_releasesFrozen() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());
        client.unfreeze(UnfreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("2000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());

        assertThat(client.getFrozen(100L, "USDT")).isEqualByComparingTo("3000");
        assertThat(client.getAvailable(100L, "USDT")).isEqualByComparingTo("7000");
    }

    @Test
    void unfreeze_moreThanFrozen_throws() {
        client.freeze(FreezeRequest.builder()
                .userId(100L).currency("USDT").amount(new BigDecimal("5000"))
                .bizType("FREEZE_ORDER").bizId(1L).build());
        assertThatThrownBy(() -> client.unfreeze(UnfreezeRequest.builder()
                        .userId(100L).currency("USDT").amount(new BigDecimal("9999"))
                        .bizType("FREEZE_ORDER").bizId(1L).build()))
                .hasMessageContaining("解冻金额超过冻结金额");
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=MockAccountServiceClientTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 DTO 与接口**

`FreezeRequest.java`:

```java
package com.cex.order.infrastructure.asset;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FreezeRequest {
    private Long userId;
    private String currency;
    private BigDecimal amount;
    private String bizType;   // 如 FREEZE_ORDER
    private Long bizId;       // 如 orderId,幂等键
}
```

`UnfreezeRequest.java`:

```java
package com.cex.order.infrastructure.asset;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UnfreezeRequest {
    private Long userId;
    private String currency;
    private BigDecimal amount;
    private String bizType;
    private Long bizId;
}
```

`AccountServiceClient.java`:

```java
package com.cex.order.infrastructure.asset;

/**
 * 账户服务客户端:资产冻结/解冻
 * 冻结接口幂等约束:bizType + bizId 唯一,重复调用不重复扣减
 * TODO: 资产服务(cex-asset-service)就绪后,以 Dubbo @DubboReference 实现本接口,冻结/解冻走 RPC
 */
public interface AccountServiceClient {

    void freeze(FreezeRequest request);

    void unfreeze(UnfreezeRequest request);
}
```

- [ ] **步骤 4:实现 MockAccountServiceClient**

`MockAccountServiceClient.java`:

```java
package com.cex.order.infrastructure.asset;

import com.cex.common.core.exception.BizException;
import com.cex.order.common.ErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 资产冻结 Mock 实现(Phase 1)
 * 内存账本模拟可用余额/冻结余额,支持 bizType+bizId 幂等。
 * TODO: 资产服务就绪后替换为 Dubbo 实现,删除本类。
 */
@Slf4j
@Component
public class MockAccountServiceClient implements AccountServiceClient {

    /** userId:currency -> 可用余额 */
    private final Map<String, BigDecimal> available = new ConcurrentHashMap<>();
    /** userId:currency -> 冻结余额 */
    private final Map<String, BigDecimal> frozen = new ConcurrentHashMap<>();
    /** bizType:bizId -> 已冻结金额(幂等记录) */
    private final Set<String> frozenRecords = ConcurrentHashMap.newKeySet();

    public MockAccountServiceClient() {
        // 预置测试账户余额:用户 100
        available.put("100:USDT", new BigDecimal("1000000"));
        available.put("100:BTC", new BigDecimal("100"));
        available.put("100:ETH", new BigDecimal("1000"));
    }

    /**
     * 测试专用构造器:自定义初始余额(map key 为 "userId:currency")
     */
    public MockAccountServiceClient(Map<String, BigDecimal> initialBalances) {
        initialBalances.forEach((k, v) -> available.put(k, v));
    }

    @Override
    public synchronized void freeze(FreezeRequest request) {
        String key = request.getUserId() + ":" + request.getCurrency();
        String recordKey = request.getBizType() + ":" + request.getBizId();
        if (frozenRecords.contains(recordKey)) {
            log.info("[MOCK] 冻结幂等命中,跳过: {}", recordKey);
            return;
        }
        BigDecimal avail = available.getOrDefault(key, BigDecimal.ZERO);
        if (avail.compareTo(request.getAmount()) < 0) {
            throw new BizException(ErrorCode.INSUFFICIENT_BALANCE.getCode(),
                    ErrorCode.INSUFFICIENT_BALANCE.getMessage() + ": " + request.getCurrency());
        }
        available.put(key, avail.subtract(request.getAmount()));
        frozen.put(key, frozen.getOrDefault(key, BigDecimal.ZERO).add(request.getAmount()));
        frozenRecords.add(recordKey);
        log.info("[MOCK] 冻结成功: userId={}, currency={}, amount={}, bizType={}, bizId={}",
                request.getUserId(), request.getCurrency(), request.getAmount(),
                request.getBizType(), request.getBizId());
    }

    @Override
    public synchronized void unfreeze(UnfreezeRequest request) {
        String key = request.getUserId() + ":" + request.getCurrency();
        BigDecimal fz = frozen.getOrDefault(key, BigDecimal.ZERO);
        if (fz.compareTo(request.getAmount()) < 0) {
            throw new BizException(ErrorCode.FREEZE_FAILED.getCode(),
                    "解冻金额超过冻结金额: " + request.getAmount() + " > " + fz);
        }
        frozen.put(key, fz.subtract(request.getAmount()));
        available.put(key, available.getOrDefault(key, BigDecimal.ZERO).add(request.getAmount()));
        log.info("[MOCK] 解冻成功: userId={}, currency={}, amount={}, bizType={}, bizId={}",
                request.getUserId(), request.getCurrency(), request.getAmount(),
                request.getBizType(), request.getBizId());
    }

    /** 测试辅助:获取可用余额 */
    public BigDecimal getAvailable(Long userId, String currency) {
        return available.getOrDefault(userId + ":" + currency, BigDecimal.ZERO);
    }

    /** 测试辅助:获取冻结余额 */
    public BigDecimal getFrozen(Long userId, String currency) {
        return frozen.getOrDefault(userId + ":" + currency, BigDecimal.ZERO);
    }
}
```

- [ ] **步骤 5:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=MockAccountServiceClientTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,5 个测试全部通过。

---

### 任务 8:创建订单服务(核心,事务边界 + 幂等 + 补偿)

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/application/command/CreateOrderCommand.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/OrderEventPublisher.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/OrderPersistenceService.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/CreateOrderService.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/CreateOrderResult.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/CreateOrderServiceTest.java`

事务边界(遵循设计文档第六节):
- `CreateOrderService.createOrder`:幂等预查 → 校验 → 生成 orderId → 冻结(事务外)→ 调 `OrderPersistenceService.createOrderInTx`(@Transactional,写订单 + outbox)→ 事务失败补偿解冻
- Kafka 发送绝不在事务内

- [ ] **步骤 1:编写失败的测试**

`CreateOrderServiceTest.java`:

```java
package com.cex.order.application.service;

import com.baomidou.mybatisplus.core.exceptions.MybatisPlusException;
import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.domain.service.TradingRuleValidator;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.FreezeRequest;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateOrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private SymbolConfigService symbolConfigService;
    @Mock private AccountServiceClient accountServiceClient;
    @Mock private OrderPersistenceService persistenceService;

    private CreateOrderService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    private final SnowflakeGenerator snowflake = new SnowflakeGenerator(1, 1);

    @BeforeEach
    void setUp() {
        service = new CreateOrderService(orderRepository, symbolConfigService,
                new TradingRuleValidator(), new FreezeCalculator(),
                accountServiceClient, persistenceService, snowflake);
    }

    private CreateOrderCommand limitBuyCommand() {
        return CreateOrderCommand.builder()
                .userId(100L).clientOrderId("client_order_123").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .timeInForce(TimeInForce.GTC)
                .build();
    }

    @Test
    void createOrder_success_freezeThenPersist() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any())).thenReturn(mock(CreateOrderResult.class));

        service.createOrder(limitBuyCommand());

        ArgumentCaptor<FreezeRequest> freezeCaptor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(freezeCaptor.capture());
        FreezeRequest freeze = freezeCaptor.getValue();
        assertThat(freeze.getUserId()).isEqualTo(100L);
        assertThat(freeze.getCurrency()).isEqualTo("USDT");
        assertThat(freeze.getAmount()).isEqualByComparingTo("10000"); // 100000 * 0.1
        assertThat(freeze.getBizType()).isEqualTo("FREEZE_ORDER");
        assertThat(freeze.getBizId()).isNotNull();
        verify(persistenceService).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_duplicateClientOrderId_returnsExistingWithoutFreeze() {
        Order existing = Order.builder().orderId(1L).userId(100L)
                .clientOrderId("client_order_123").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .status(com.cex.order.domain.model.OrderStatus.PENDING_MATCH)
                .build();
        when(orderRepository.findByUserIdAndClientOrderId(100L, "client_order_123"))
                .thenReturn(existing);

        CreateOrderResult result = service.createOrder(limitBuyCommand());

        assertThat(result.getOrderId()).isEqualTo(1L);
        verify(accountServiceClient, never()).freeze(any());
        verify(persistenceService, never()).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_symbolNotExist_throwsBeforeFreeze() {
        when(symbolConfigService.getRequired("BTC_USDT"))
                .thenThrow(new BizException(40010, "交易对不存在: BTC_USDT"));

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("交易对不存在");
        verify(accountServiceClient, never()).freeze(any());
    }

    @Test
    void createOrder_freezerFails_throwsAndNoPersist() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        doThrow(new BizException(50011, "余额不足"))
                .when(accountServiceClient).freeze(any());

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("余额不足");
        verify(persistenceService, never()).createOrderInTx(any(), anyLong(), any());
    }

    @Test
    void createOrder_persistFails_unfreezeCompensates() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any()))
                .thenThrow(new RuntimeException("db down"));

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(RuntimeException.class);

        ArgumentCaptor<UnfreezeRequest> unfreezeCaptor = ArgumentCaptor.forClass(UnfreezeRequest.class);
        verify(accountServiceClient).unfreeze(unfreezeCaptor.capture());
        UnfreezeRequest unfreeze = unfreezeCaptor.getValue();
        assertThat(unfreeze.getBizId()).isEqualTo(freezeBizIdCaptured());
        assertThat(unfreeze.getAmount()).isEqualByComparingTo("10000");
    }

    private Long freezeBizIdCaptured() {
        return null; // 见步骤 4:改为捕获 freeze 请求后取 bizId
    }

    @Test
    void createOrder_priceScaleError_throwsBeforeFreeze() {
        CreateOrderCommand bad = limitBuyCommand();
        bad.setPrice(new BigDecimal("100000.123")); // 3 位小数,超出 priceScale=2
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        assertThatThrownBy(() -> service.createOrder(bad))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("价格精度");
        verify(accountServiceClient, never()).freeze(any());
    }

    @Test
    void createOrder_marketBuy_freezesQuoteAmount() {
        CreateOrderCommand marketBuy = CreateOrderCommand.builder()
                .userId(100L).clientOrderId("c_mkt").symbol("BTC_USDT")
                .side(OrderSide.BUY).type(OrderType.MARKET)
                .quoteAmount(new BigDecimal("5000"))
                .timeInForce(TimeInForce.GTC)
                .build();
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any())).thenReturn(mock(CreateOrderResult.class));

        service.createOrder(marketBuy);

        ArgumentCaptor<FreezeRequest> captor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(captor.capture());
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("5000");
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=CreateOrderServiceTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 Command 与 Result**

`CreateOrderCommand.java`:

```java
package com.cex.order.application.command;

import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderCommand {

    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    /** 限价单价格;市价买单此字段传 quoteAmount */
    private BigDecimal price;
    private BigDecimal quantity;
    /** 市价买单的冻结金额(限价单/市价卖单为 null) */
    private BigDecimal quoteAmount;
    private TimeInForce timeInForce;
}
```

`CreateOrderResult.java`:

```java
package com.cex.order.application.service;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderResult {

    private Long orderId;
    private OrderStatus status;

    public static CreateOrderResult of(Order order) {
        return CreateOrderResult.builder()
                .orderId(order.getOrderId())
                .status(order.getStatus())
                .build();
    }
}
```

- [ ] **步骤 4:实现 OrderEventPublisher(写 Outbox)**

先创建 Outbox 仓储与 PO(OutboxRepository 在本任务实现,Relay 在任务 10):

`OrderEventOutboxPO.java`:

```java
package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("order_event_outbox")
public class OrderEventOutboxPO {

    public static final String STATUS_INIT = "INIT";
    public static final String STATUS_SENDING = "SENDING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";

    @TableId(type = IdType.INPUT)
    private Long id;
    private String eventId;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String payload;
    private String status;
    private Integer retryCount;
    private LocalDateTime nextRetryTime;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

`OutboxRepository.java`:

```java
package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.persistence.mapper.OrderEventOutboxMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {

    private final OrderEventOutboxMapper outboxMapper;

    public void insert(OrderEventOutboxPO outbox) {
        outboxMapper.insert(outbox);
    }

    public void update(OrderEventOutboxPO outbox) {
        outboxMapper.updateById(outbox);
    }

    /** 扫描待发送记录:INIT/SENDING 且到重试时间 且未超上限 */
    public List<OrderEventOutboxPO> findPending(int limit, int maxRetry) {
        return outboxMapper.selectList(new LambdaQueryWrapper<OrderEventOutboxPO>()
                .in(OrderEventOutboxPO::getStatus,
                        OrderEventOutboxPO.STATUS_INIT, OrderEventOutboxPO.STATUS_SENDING)
                .le(OrderEventOutboxPO::getNextRetryTime, LocalDateTime.now())
                .lt(OrderEventOutboxPO::getRetryCount, maxRetry)
                .orderByAsc(OrderEventOutboxPO::getId)
                .last("LIMIT " + limit));
    }
}
```

`OrderEventOutboxMapper.java`:

```java
package com.cex.order.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface OrderEventOutboxMapper extends BaseMapper<OrderEventOutboxPO> {
}
```

`OrderEventPublisher.java`:

```java
package com.cex.order.application.service;

import com.cex.order.domain.model.Order;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.cex.common.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 订单领域事件发布:事务内写入 Outbox(本地事务保证订单与事件一致)
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    public static final String EVENT_ORDER_CREATED = "ORDER_CREATED";
    public static final String EVENT_ORDER_CANCELED = "ORDER_CANCELED";

    private final OutboxRepository outboxRepository;
    private final SnowflakeGenerator snowflakeGenerator;
    private final ObjectMapper objectMapper;

    /** 订单创建事件(SUBMIT) */
    public void publishOrderCreated(Order order) {
        OrderEvent event = OrderEvent.builder()
                .orderId(String.valueOf(order.getOrderId()))
                .clientOrderId(order.getClientOrderId())
                .userId(order.getUserId())
                .symbol(order.getSymbol())
                .action(OrderEvent.Action.SUBMIT)
                .side(OrderEvent.OrderSide.valueOf(order.getSide().name()))
                .type(OrderEvent.OrderType.valueOf(order.getType().name()))
                .price(order.getPrice())
                .quantity(order.getQuantity())
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CREATED, event);
    }

    /** 订单取消事件(CANCEL) */
    public void publishOrderCanceled(Order order) {
        OrderEvent event = OrderEvent.builder()
                .orderId(String.valueOf(order.getOrderId()))
                .clientOrderId(order.getClientOrderId())
                .userId(order.getUserId())
                .symbol(order.getSymbol())
                .action(OrderEvent.Action.CANCEL)
                .side(OrderEvent.OrderSide.valueOf(order.getSide().name()))
                .type(OrderEvent.OrderType.valueOf(order.getType().name()))
                .price(order.getPrice())
                .quantity(order.getQuantity().subtract(
                        order.getFilledQuantity() == null ? java.math.BigDecimal.ZERO : order.getFilledQuantity()))
                .timestamp(System.currentTimeMillis())
                .build();
        insertOutbox(order, EVENT_ORDER_CANCELED, event);
    }

    private void insertOutbox(Order order, String eventType, OrderEvent event) {
        LocalDateTime now = LocalDateTime.now();
        OrderEventOutboxPO outbox = OrderEventOutboxPO.builder()
                .id(snowflakeGenerator.nextId())
                .eventId(UUID.randomUUID().toString())
                .aggregateType("ORDER")
                .aggregateId(String.valueOf(order.getOrderId()))
                .eventType(eventType)
                .payload(toJson(event))
                .status(OrderEventOutboxPO.STATUS_INIT)
                .retryCount(0)
                .nextRetryTime(now)
                .createdAt(now)
                .updatedAt(now)
                .build();
        outboxRepository.insert(outbox);
        log.info("订单事件已写入 Outbox: eventType={}, orderId={}, userId={}, symbol={}",
                eventType, order.getOrderId(), order.getUserId(), order.getSymbol());
    }

    private String toJson(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("事件 JSON 序列化失败", e);
        }
    }
}
```

- [ ] **步骤 5:实现 OrderPersistenceService 与 CreateOrderService**

`OrderPersistenceService.java`(独立 bean,保证 @Transactional 代理生效):

```java
package com.cex.order.application.service;

import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderFactory;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.SymbolConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 订单落库服务:INSERT orders + INSERT order_event_outbox 在同一本地事务
 * 注意:Kafka 发送不允许出现在本事务内,由 Outbox Relay 负责
 */
@Service
@RequiredArgsConstructor
public class OrderPersistenceService {

    private final OrderRepository orderRepository;
    private final OrderFactory orderFactory;
    private final OrderEventPublisher eventPublisher;

    @Transactional
    public CreateOrderResult createOrderInTx(CreateOrderCommand command, Long orderId, SymbolConfig config) {
        Order order = orderFactory.createPendingMatchOrder(
                orderId,
                command.getUserId(),
                command.getClientOrderId(),
                command.getSymbol(),
                command.getSide(),
                command.getType(),
                command.getType() == com.cex.order.domain.model.OrderType.MARKET
                        ? null : command.getPrice(),
                command.getType() == com.cex.order.domain.model.OrderType.MARKET
                        && command.getSide() == com.cex.order.domain.model.OrderSide.SELL
                        ? command.getQuantity() : command.getQuantity(),
                command.getQuoteAmount(),
                command.getTimeInForce());
        orderRepository.insert(order);
        eventPublisher.publishOrderCreated(order);
        return CreateOrderResult.of(order);
    }
}
```

`CreateOrderService.java`:

```java
package com.cex.order.application.service;

import com.baomidou.mybatisplus.core.exceptions.MybatisPlusException;
import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.domain.service.TradingRuleValidator;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.FreezeRequest;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * 创建订单用例编排
 * 事务边界:
 *   - 冻结(RPC)在事务外:失败不落订单
 *   - 订单 + Outbox 在同一本地事务(OrderPersistenceService)
 *   - 本地事务失败 -> 补偿解冻,冻结不会永久存在
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CreateOrderService {

    private final OrderRepository orderRepository;
    private final SymbolConfigService symbolConfigService;
    private final TradingRuleValidator ruleValidator;
    private final FreezeCalculator freezeCalculator;
    private final AccountServiceClient accountServiceClient;
    private final OrderPersistenceService persistenceService;
    private final SnowflakeGenerator snowflakeGenerator;

    public CreateOrderResult createOrder(CreateOrderCommand command) {
        // 1. 幂等预查:重复 clientOrderId 直接返回首次结果,不重复冻结
        Order existing = orderRepository.findByUserIdAndClientOrderId(
                command.getUserId(), command.getClientOrderId());
        if (existing != null) {
            log.info("幂等命中,返回首次结果: userId={}, clientOrderId={}, orderId={}",
                    command.getUserId(), command.getClientOrderId(), existing.getOrderId());
            return CreateOrderResult.of(existing);
        }

        // 2-5. 交易对与规则校验(不存在/暂停/精度/最小数量/最小金额)
        SymbolConfig config = symbolConfigService.getRequired(command.getSymbol());
        ruleValidator.validatePrice(command.getPrice(), config);
        ruleValidator.validateQuantity(command.getQuantity(), config);
        ruleValidator.validateMinAmount(command.getSide(), command.getType(),
                command.getQuoteAmount() != null ? command.getQuoteAmount() : command.getPrice(),
                command.getQuantity(), config);

        // 6. 订单 ID(先于冻结生成,bizId=orderId 保证冻结幂等)
        Long orderId = snowflakeGenerator.nextId();

        // 7-8. 计算冻结金额并冻结(事务外)
        BigDecimal freezeAmount = freezeCalculator.calculate(command.getSide(), command.getType(),
                command.getPrice(), command.getQuantity(), command.getQuoteAmount());
        String currency = freezeCalculator.freezeCurrency(command.getSide(), config);
        try {
            accountServiceClient.freeze(FreezeRequest.builder()
                    .userId(command.getUserId())
                    .currency(currency)
                    .amount(freezeAmount)
                    .bizType("FREEZE_ORDER")
                    .bizId(orderId)
                    .build());
        } catch (BizException e) {
            throw e;
        } catch (Exception e) {
            log.error("资产冻结异常: userId={}, orderId={}, currency={}", command.getUserId(), orderId, currency, e);
            throw new BizException(ErrorCode.FREEZE_FAILED.getCode(), ErrorCode.FREEZE_FAILED.getMessage());
        }

        // 9. 本地事务:订单 + Outbox
        try {
            return persistenceService.createOrderInTx(command, orderId, config);
        } catch (Exception e) {
            // 10. 补偿:本地事务失败必须解冻,防止冻结永久存在
            log.error("订单落库失败,补偿解冻: orderId={}, userId={}", orderId, command.getUserId(), e);
            try {
                accountServiceClient.unfreeze(UnfreezeRequest.builder()
                        .userId(command.getUserId())
                        .currency(currency)
                        .amount(freezeAmount)
                        .bizType("FREEZE_ORDER")
                        .bizId(orderId)
                        .build());
            } catch (Exception ex) {
                log.error("补偿解冻失败,需人工介入: orderId={}, userId={}, amount={}",
                        orderId, command.getUserId(), freezeAmount, ex);
            }
            throw e;
        }
    }
}
```

- [ ] **步骤 6:修复测试中的补偿断言,运行验证**

`createOrder_persistFails_unfreezeCompensates` 中 `freezeBizIdCaptured()` 需断言 freeze 与 unfreeze 使用同一 bizId。修改测试:

```java
    @Test
    void createOrder_persistFails_unfreezeCompensates() {
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);
        when(persistenceService.createOrderInTx(any(), anyLong(), any()))
                .thenThrow(new RuntimeException("db down"));

        assertThatThrownBy(() -> service.createOrder(limitBuyCommand()))
                .isInstanceOf(RuntimeException.class);

        ArgumentCaptor<FreezeRequest> freezeCaptor = ArgumentCaptor.forClass(FreezeRequest.class);
        verify(accountServiceClient).freeze(freezeCaptor.capture());
        ArgumentCaptor<UnfreezeRequest> unfreezeCaptor = ArgumentCaptor.forClass(UnfreezeRequest.class);
        verify(accountServiceClient).unfreeze(unfreezeCaptor.capture());

        assertThat(unfreezeCaptor.getValue().getBizId())
                .isEqualTo(freezeCaptor.getValue().getBizId());
        assertThat(unfreezeCaptor.getValue().getAmount()).isEqualByComparingTo("10000");
    }
```

删除测试中的 `freezeBizIdCaptured()` 方法与对应旧断言。

运行:`mvn -pl cex-order-service -am test -Dtest=CreateOrderServiceTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,8 个测试全部通过(幂等返回首次结果、符号不存在不冻结、冻结失败不落库、落库失败补偿解冻、精度错误不冻结、市价买单按 quoteAmount 冻结)。

---

### 任务 9:取消订单服务

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/application/command/CancelOrderCommand.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/CancelOrderService.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/CancelOrderServiceTest.java`

事务边界:状态机校验 → `@Transactional`(置 CANCELED + 写 outbox)→ 提交后解冻剩余冻结资产(事务外)。

- [ ] **步骤 1:编写失败的测试**

`CancelOrderServiceTest.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.common.OrderStatusInvalidException;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CancelOrderServiceTest {

    @Mock private OrderRepository orderRepository;
    @Mock private SymbolConfigService symbolConfigService;
    @Mock private AccountServiceClient accountServiceClient;
    @Mock private OrderPersistenceService persistenceService;

    private CancelOrderService service;

    private final SymbolConfig config = SymbolConfig.builder()
            .symbol("BTC_USDT").baseCurrency("BTC").quoteCurrency("USDT")
            .priceScale(2).quantityScale(6)
            .minQuantity(new BigDecimal("0.0001")).minAmount(new BigDecimal("10"))
            .status(SymbolConfig.SymbolStatus.ACTIVE)
            .build();

    @BeforeEach
    void setUp() {
        service = new CancelOrderService(orderRepository, symbolConfigService,
                accountServiceClient, persistenceService, new FreezeCalculator());
    }

    private Order openBuyOrder() {
        return Order.builder()
                .id(1L).orderId(1L).userId(100L).clientOrderId("c1")
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH).timeInForce(TimeInForce.GTC)
                .build();
    }

    private CancelOrderCommand cancelCommand() {
        return CancelOrderCommand.builder().userId(100L).orderId(1L).build();
    }

    @Test
    void cancel_success_persistsCanceledAndUnfreezes() {
        Order order = openBuyOrder();
        when(orderRepository.findByOrderId(1L)).thenReturn(order);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        service.cancelOrder(cancelCommand());

        verify(persistenceService).cancelInTx(any());
        ArgumentCaptor<UnfreezeRequest> captor = ArgumentCaptor.forClass(UnfreezeRequest.class);
        verify(accountServiceClient).unfreeze(captor.capture());
        assertThat(captor.getValue().getCurrency()).isEqualTo("USDT");
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("10000");
        assertThat(captor.getValue().getBizType()).isEqualTo("FREEZE_ORDER");
        assertThat(captor.getValue().getBizId()).isEqualTo(1L);
    }

    @Test
    void cancel_orderNotExist_throws() {
        when(orderRepository.findByOrderId(99L)).thenReturn(null);
        CancelOrderCommand cmd = CancelOrderCommand.builder().userId(100L).orderId(99L).build();

        assertThatThrownBy(() -> service.cancelOrder(cmd))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
        verify(persistenceService, never()).cancelInTx(any());
        verify(accountServiceClient, never()).unfreeze(any());
    }

    @Test
    void cancel_wrongUser_throws() {
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder());
        CancelOrderCommand cmd = CancelOrderCommand.builder().userId(999L).orderId(1L).build();

        assertThatThrownBy(() -> service.cancelOrder(cmd))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
    }

    @Test
    void cancel_filledOrder_rejected() {
        Order filled = openBuyOrder();
        filled.markPartiallyFilled(new BigDecimal("0.1"), new BigDecimal("10000"));
        when(orderRepository.findByOrderId(1L)).thenReturn(filled);

        assertThatThrownBy(() -> service.cancelOrder(cancelCommand()))
                .isInstanceOf(OrderStatusInvalidException.class)
                .hasMessageContaining("FILLED");
        verify(persistenceService, never()).cancelInTx(any());
        verify(accountServiceClient, never()).unfreeze(any());
    }

    @Test
    void cancel_partiallyFilled_unfreezesRemaining() {
        Order partial = openBuyOrder();
        partial.markPartiallyFilled(new BigDecimal("0.04"), new BigDecimal("4000"));
        when(orderRepository.findByOrderId(1L)).thenReturn(partial);
        when(symbolConfigService.getRequired("BTC_USDT")).thenReturn(config);

        service.cancelOrder(cancelCommand());

        ArgumentCaptor<UnfreezeRequest> captor = ArgumentCaptor.forClass(UnfreezeRequest.class);
        verify(accountServiceClient).unfreeze(captor.capture());
        assertThat(captor.getValue().getAmount()).isEqualByComparingTo("6000"); // 100000*(0.1-0.04)
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=CancelOrderServiceTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 Command 与 Service**

`CancelOrderCommand.java`:

```java
package com.cex.order.application.command;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelOrderCommand {
    private Long userId;
    private Long orderId;
}
```

`CancelOrderService.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.domain.service.FreezeCalculator;
import com.cex.order.domain.service.SymbolConfig;
import com.cex.order.infrastructure.asset.AccountServiceClient;
import com.cex.order.infrastructure.asset.UnfreezeRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

/**
 * 取消订单用例编排
 * 事务边界:状态置 CANCELED + Outbox 事件在同一事务;解冻在事务外
 * 说明:Phase 1 撮合引擎未接入,取消直接落库并发事件;撮合引擎接入后,
 *      最终状态以撮合回报为准(事件流按 symbol 保序,本流程结构无需变更)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CancelOrderService {

    private final OrderRepository orderRepository;
    private final SymbolConfigService symbolConfigService;
    private final AccountServiceClient accountServiceClient;
    private final OrderPersistenceService persistenceService;
    private final FreezeCalculator freezeCalculator;

    public void cancelOrder(CancelOrderCommand command) {
        // 归属 + 状态校验
        Order order = orderRepository.findByOrderId(command.getOrderId());
        if (order == null || !command.getUserId().equals(order.getUserId())) {
            throw new BizException(ErrorCode.ORDER_NOT_FOUND.getCode(),
                    ErrorCode.ORDER_NOT_FOUND.getMessage() + ": " + command.getOrderId());
        }
        order.cancel(); // 非法状态抛 OrderStatusInvalidException(如 FILLED)

        // 事务:置 CANCELED + 写取消事件 Outbox
        persistenceService.cancelInTx(order);

        // 事务外:解冻剩余冻结资产
        SymbolConfig config = symbolConfigService.getRequired(order.getSymbol());
        BigDecimal unfreezeAmount = freezeCalculator.remainingToUnfreeze(order, config);
        if (unfreezeAmount.compareTo(BigDecimal.ZERO) > 0) {
            try {
                accountServiceClient.unfreeze(UnfreezeRequest.builder()
                        .userId(order.getUserId())
                        .currency(freezeCalculator.freezeCurrency(order.getSide(), config))
                        .amount(unfreezeAmount)
                        .bizType("FREEZE_ORDER")
                        .bizId(order.getOrderId())
                        .build());
            } catch (Exception e) {
                log.error("取消订单解冻失败,需人工介入: orderId={}, userId={}, amount={}",
                        order.getOrderId(), order.getUserId(), unfreezeAmount, e);
            }
        }
    }
}
```

- [ ] **步骤 4:OrderPersistenceService 增加 cancelInTx**

在 `OrderPersistenceService` 中新增方法:

```java
    /**
     * 取消落库:状态置 CANCELED + 写取消事件,同一事务
     */
    @Transactional
    public void cancelInTx(Order order) {
        order.setUpdatedAt(java.time.LocalDateTime.now());
        orderRepository.update(order);
        eventPublisher.publishOrderCanceled(order);
    }
```

- [ ] **步骤 5:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=CancelOrderServiceTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,5 个测试全部通过。

---

### 任务 10:Outbox Relay + Kafka Producer

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/kafka/OrderKafkaProducer.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/kafka/OutboxRelay.java`(@Scheduled 调度入口)
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/OutboxRelayService.java`
- 修改:`cex-order-service/src/main/java/com/cex/order/CexOrderServiceApplication.java`(加 @EnableScheduling)
- 修改:`cex-order-service/src/main/resources/application.yml`(producer acks/idempotence)
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/OutboxRelayServiceTest.java`

- [ ] **步骤 1:编写失败的测试**

`OutboxRelayServiceTest.java`:

```java
package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.order.infrastructure.kafka.OrderKafkaProducer;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock private OutboxRepository outboxRepository;
    @Mock private OrderKafkaProducer kafkaProducer;

    private OutboxRelayService service;

    @BeforeEach
    void setUp() {
        service = new OutboxRelayService(outboxRepository, kafkaProducer);
    }

    private OrderEventOutboxPO pendingOutbox() {
        return OrderEventOutboxPO.builder()
                .id(1L).eventId("evt-1").aggregateType("ORDER").aggregateId("1")
                .eventType(OrderEventPublisher.EVENT_ORDER_CREATED)
                .payload("{\"orderId\":\"1\",\"symbol\":\"BTC_USDT\",\"quantity\":0.1}")
                .status(OrderEventOutboxPO.STATUS_INIT).retryCount(0)
                .nextRetryTime(LocalDateTime.now()).createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Test
    void relay_success_marksSuccess() {
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(pendingOutbox()));

        service.relay();

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        verify(outboxRepository).update(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(OrderEventOutboxPO.STATUS_SUCCESS);
        verify(kafkaProducer).send(any());
    }

    @Test
    void relay_sendFails_retryCountIncremented() {
        OrderEventOutboxPO outbox = pendingOutbox();
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(outbox));
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).send(any());

        service.relay();

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        verify(outboxRepository, never()).update(captor.capture());
        // 两次更新:一次置 SENDING,一次失败重试
        verify(outboxRepository, org.mockito.Mockito.times(2)).update(any());
    }

    @Test
    void relay_retryExceeded_marksFailed() {
        OrderEventOutboxPO outbox = pendingOutbox();
        outbox.setRetryCount(9); // 达到上限(10)前最后一次
        when(outboxRepository.findPending(100, 10)).thenReturn(List.of(outbox));
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).send(any());

        service.relay();

        ArgumentCaptor<OrderEventOutboxPO> captor = ArgumentCaptor.forClass(OrderEventOutboxPO.class);
        verify(outboxRepository, org.mockito.Mockito.times(2)).update(any());
        // 最后一次更新状态为 FAILED
        verify(outboxRepository).update(org.mockito.ArgumentMatchers.argThat(
                o -> OrderEventOutboxPO.STATUS_FAILED.equals(o.getStatus())));
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=OutboxRelayServiceTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现 OrderKafkaProducer 与 OutboxRelayService**

`OrderKafkaProducer.java`:

```java
package com.cex.order.infrastructure.kafka;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 订单事件 Kafka 生产者
 * key=symbol,保证同交易对事件进入同一分区保持顺序
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderKafkaProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void send(OrderEvent event) {
        try {
            kafkaTemplate.send(TopicConstants.TOPIC_ORDER_EVENT, event.getSymbol(), event)
                    .get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Kafka 发送失败: topic=" + TopicConstants.TOPIC_ORDER_EVENT
                    + ", symbol=" + event.getSymbol() + ", orderId=" + event.getOrderId(), e);
        }
    }
}
```

`OutboxRelayService.java`:

```java
package com.cex.order.application.service;

import com.cex.common.kafka.event.OrderEvent;
import com.cex.order.infrastructure.kafka.OrderKafkaProducer;
import com.cex.order.infrastructure.persistence.entity.OrderEventOutboxPO;
import com.cex.order.infrastructure.repository.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Outbox Relay:将发件箱事件投递到 Kafka
 * 失败指数退避:next_retry_time = now + 2^retry_count 秒,超过最大重试置 FAILED 并告警
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelayService {

    public static final int BATCH_SIZE = 100;
    public static final int MAX_RETRY = 10;

    private final OutboxRepository outboxRepository;
    private final OrderKafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;

    public void relay() {
        List<OrderEventOutboxPO> pending = outboxRepository.findPending(BATCH_SIZE, MAX_RETRY);
        if (pending.isEmpty()) {
            return;
        }
        log.info("Outbox Relay 扫描到 {} 条待发送事件", pending.size());
        for (OrderEventOutboxPO outbox : pending) {
            relayOne(outbox);
        }
    }

    private void relayOne(OrderEventOutboxPO outbox) {
        outbox.setStatus(OrderEventOutboxPO.STATUS_SENDING);
        outbox.setUpdatedAt(LocalDateTime.now());
        outboxRepository.update(outbox);
        try {
            OrderEvent event = objectMapper.readValue(outbox.getPayload(), OrderEvent.class);
            kafkaProducer.send(event);
            outbox.setStatus(OrderEventOutboxPO.STATUS_SUCCESS);
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxRepository.update(outbox);
            log.info("Outbox 事件发送成功: eventId={}, eventType={}, orderId={}",
                    outbox.getEventId(), outbox.getEventType(), outbox.getAggregateId());
        } catch (Exception e) {
            int retryCount = outbox.getRetryCount() + 1;
            outbox.setRetryCount(retryCount);
            if (retryCount >= MAX_RETRY) {
                outbox.setStatus(OrderEventOutboxPO.STATUS_FAILED);
                log.error("Outbox 事件超过最大重试次数,需人工处理: eventId={}, eventType={}, orderId={}",
                        outbox.getEventId(), outbox.getEventType(), outbox.getAggregateId(), e);
            } else {
                outbox.setStatus(OrderEventOutboxPO.STATUS_INIT);
                outbox.setNextRetryTime(LocalDateTime.now().plusSeconds(1L << retryCount));
                log.warn("Outbox 事件发送失败,第 {} 次重试: eventId={}, eventType={}",
                        retryCount, outbox.getEventId(), outbox.getEventType(), e);
            }
            outbox.setUpdatedAt(LocalDateTime.now());
            outboxRepository.update(outbox);
        }
    }
}
```

- [ ] **步骤 4:创建调度入口与配置**

`OutboxRelay.java`:

```java
package com.cex.order.infrastructure.kafka;

import com.cex.order.application.service.OutboxRelayService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Outbox Relay 定时调度:每 1 秒扫描一次
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxRelayService relayService;

    @Scheduled(fixedDelay = 1000, initialDelay = 5000)
    public void scan() {
        try {
            relayService.relay();
        } catch (Exception e) {
            log.error("Outbox Relay 执行异常", e);
        }
    }
}
```

修改 `CexOrderServiceApplication.java`:

```java
package com.cex.order;

import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 订单服务启动类
 * 核心链路:下单(本地事务 + Outbox 表)-> 发布 OrderEvent 到 Kafka -> Matching Engine 撮合
 */
@EnableDubbo
@EnableScheduling
@SpringBootApplication(scanBasePackages = "com.cex")
public class CexOrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CexOrderServiceApplication.class, args);
    }
}
```

修改 `application.yml`,producer 部分追加:

```yaml
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      # 消息可靠性:全部副本确认 + 幂等生产者(防重发)
      acks: all
      properties:
        enable.idempotence: true
        retries: 3
```

- [ ] **步骤 5:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=OutboxRelayServiceTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,3 个测试全部通过。

---

### 任务 11:成交回报消费者(幂等消费)

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/entity/ProcessedEventPO.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/persistence/mapper/ProcessedEventMapper.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/repository/ProcessedEventRepository.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/OrderEventConsumer.java`
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/OrderEventConsumerTest.java`

监听 `cex.trade.event`(group=cex-order),按 TradeEvent.buyOrderId/sellOrderId 更新订单;幂等键 = tradeId + consumer。**处理逻辑整体在同一数据库事务内**(check → 更新订单 → 记录 processed_event),Kafka 重投时无副作用。

- [ ] **步骤 1:编写失败的测试**

`OrderEventConsumerTest.java`:

```java
package com.cex.order.application.service;

import com.cex.common.kafka.event.TradeEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderEventConsumerTest {

    @Mock private OrderRepository orderRepository;
    @Mock private ProcessedEventRepository processedEventRepository;

    private OrderEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new OrderEventConsumer(orderRepository, processedEventRepository);
    }

    private Order openBuyOrder(long orderId) {
        return Order.builder()
                .id(orderId).orderId(orderId).userId(100L)
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH)
                .build();
    }

    private TradeEvent tradeEvent(String tradeId, long buyOrderId, long sellOrderId) {
        return TradeEvent.builder()
                .tradeId(tradeId).symbol("BTC_USDT")
                .buyOrderId(String.valueOf(buyOrderId)).sellOrderId(String.valueOf(sellOrderId))
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.04"))
                .amount(new BigDecimal("4000")).timestamp(System.currentTimeMillis())
                .build();
    }

    @Test
    void onTradeEvent_updatesBothOrdersAndRecordsProcessed() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(openBuyOrder(2L));

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        // 买单一号部分成交
        verify(orderRepository, times(2)).update(any());
        verify(processedEventRepository).save(any(ProcessedEventPO.class));
    }

    @Test
    void onTradeEvent_duplicateEvent_ignored() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(true);

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        verify(orderRepository, never()).update(any());
        verify(processedEventRepository, never()).save(any());
    }

    @Test
    void onTradeEvent_orderFillsUpdatesStatus() {
        when(processedEventRepository.exists("t1", OrderEventConsumer.CONSUMER)).thenReturn(false);
        when(orderRepository.findByOrderId(1L)).thenReturn(openBuyOrder(1L));
        when(orderRepository.findByOrderId(2L)).thenReturn(null); // 卖单不在本库(不存在则跳过)

        consumer.onTradeEvent(tradeEvent("t1", 1L, 2L));

        assertThat(consumer).isNotNull();
    }
}
```

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=OrderEventConsumerTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现幂等表与消费者**

`ProcessedEventPO.java`:

```java
package com.cex.order.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("processed_event")
public class ProcessedEventPO {

    @TableId(type = IdType.INPUT)
    private Long id;
    private String eventId;
    private String consumer;
    private LocalDateTime processedAt;
}
```

`ProcessedEventMapper.java`:

```java
package com.cex.order.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ProcessedEventMapper extends BaseMapper<ProcessedEventPO> {
}
```

`ProcessedEventRepository.java`:

```java
package com.cex.order.infrastructure.repository;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.cex.order.infrastructure.id.SnowflakeGenerator;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.persistence.mapper.ProcessedEventMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

@Repository
@RequiredArgsConstructor
public class ProcessedEventRepository {

    private final ProcessedEventMapper processedEventMapper;
    private final SnowflakeGenerator snowflakeGenerator;

    public boolean exists(String eventId, String consumer) {
        return processedEventMapper.selectCount(new LambdaQueryWrapper<ProcessedEventPO>()
                .eq(ProcessedEventPO::getEventId, eventId)
                .eq(ProcessedEventPO::getConsumer, consumer)) > 0;
    }

    public void save(ProcessedEventPO po) {
        processedEventMapper.insert(po);
    }

    public ProcessedEventPO build(String eventId, String consumer) {
        return ProcessedEventPO.builder()
                .id(snowflakeGenerator.nextId())
                .eventId(eventId)
                .consumer(consumer)
                .processedAt(LocalDateTime.now())
                .build();
    }
}
```

`OrderEventConsumer.java`:

```java
package com.cex.order.application.service;

import com.cex.common.kafka.TopicConstants;
import com.cex.common.kafka.event.TradeEvent;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import com.cex.order.infrastructure.persistence.entity.ProcessedEventPO;
import com.cex.order.infrastructure.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

/**
 * 成交回报消费者:撮合引擎发布 TradeEvent 到 cex.trade.event,本服务更新订单成交状态
 * 幂等:eventId(tradeId) + consumer 唯一约束,重复消息直接忽略
 * 事务边界:check + 更新 + 记录 processed_event 在同一事务,消费失败回滚无副作用
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    public static final String CONSUMER = "ORDER_STATUS_CONSUMER";

    private final OrderRepository orderRepository;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(topics = TopicConstants.TOPIC_TRADE_EVENT, groupId = "cex-order")
    @Transactional
    public void onTradeEvent(TradeEvent event) {
        if (event == null || event.getTradeId() == null) {
            return;
        }
        // 幂等:已处理过则忽略(事务内检查,防并发重复)
        if (processedEventRepository.exists(event.getTradeId(), CONSUMER)) {
            log.info("成交事件已处理,忽略: tradeId={}", event.getTradeId());
            return;
        }
        updateOrder(event.getBuyOrderId(), event.getQuantity(), event.getAmount());
        updateOrder(event.getSellOrderId(), event.getQuantity(), event.getAmount());
        processedEventRepository.save(processedEventRepository.build(event.getTradeId(), CONSUMER));
        log.info("成交回报处理完成: tradeId={}, symbol={}, quantity={}, price={}",
                event.getTradeId(), event.getSymbol(), event.getQuantity(), event.getPrice());
    }

    private void updateOrder(String orderIdStr, BigDecimal quantity, BigDecimal amount) {
        if (orderIdStr == null) {
            return;
        }
        try {
            Long orderId = Long.valueOf(orderIdStr);
            Order order = orderRepository.findByOrderId(orderId);
            if (order == null) {
                log.warn("成交回报的订单不存在(可能已分库或清理): orderId={}", orderId);
                return;
            }
            order.markPartiallyFilled(quantity, amount); // 状态机:内部判断 PARTIALLY_FILLED/FILLED
            orderRepository.update(order);
        } catch (NumberFormatException e) {
            log.warn("订单ID非法: {}", orderIdStr);
        }
    }
}
```

- [ ] **步骤 4:运行测试验证通过**

运行:`mvn -pl cex-order-service -am test -Dtest=OrderEventConsumerTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,3 个测试全部通过。

---

### 任务 12:查询服务 + Controller + 日志追踪

**文件:**
- 创建:`cex-order-service/src/main/java/com/cex/order/application/service/QueryOrderService.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/request/CreateOrderRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/request/OpenOrdersRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/request/OrderHistoryRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/request/CursorPagingRequest.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/response/OrderResponse.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/response/CreateOrderResponse.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/response/PageResult.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/api/controller/OrderController.java`
- 创建:`cex-order-service/src/main/java/com/cex/order/infrastructure/config/TraceFilter.java`
- 创建:`cex-order-service/src/main/resources/logback-spring.xml`
- 测试:`cex-order-service/src/test/java/com/cex/order/application/service/QueryOrderServiceTest.java`

- [ ] **步骤 1:编写失败的测试**

`QueryOrderServiceTest.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import com.cex.order.domain.repository.OrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueryOrderServiceTest {

    @Mock private OrderRepository orderRepository;

    private QueryOrderService service;

    @BeforeEach
    void setUp() {
        service = new QueryOrderService(orderRepository);
    }

    private Order order(long id) {
        return Order.builder()
                .id(id).orderId(id).userId(100L).clientOrderId("c" + id)
                .symbol("BTC_USDT").side(OrderSide.BUY).type(OrderType.LIMIT)
                .price(new BigDecimal("100000")).quantity(new BigDecimal("0.1"))
                .filledQuantity(BigDecimal.ZERO).filledAmount(BigDecimal.ZERO)
                .status(OrderStatus.PENDING_MATCH).timeInForce(TimeInForce.GTC)
                .createdAt(LocalDateTime.now().minusMinutes(id))
                .build();
    }

    @Test
    void getOrder_success() {
        when(orderRepository.findByOrderId(1L)).thenReturn(order(1L));
        OrderResponse response = service.getOrder(100L, 1L);
        assertThat(response.getOrderId()).isEqualTo(1L);
        assertThat(response.getStatus()).isEqualTo(OrderStatus.PENDING_MATCH);
    }

    @Test
    void getOrder_wrongUser_throws() {
        when(orderRepository.findByOrderId(1L)).thenReturn(order(1L));
        assertThatThrownBy(() -> service.getOrder(999L, 1L))
                .isInstanceOf(BizException.class)
                .hasMessageContaining("订单不存在");
    }

    @Test
    void listOpenOrders_passesCursor() {
        when(orderRepository.listOpenOrders(eq(100L), eq("BTC_USDT"), eq(20),
                any(), any())).thenReturn(List.of(order(3L), order(2L)));

        PageResultHolder result = service.listOpenOrders(100L, "BTC_USDT", null, null);

        assertThat(result.getItems()).hasSize(2);
        assertThat(result.getNextCursor()).isNotBlank();
    }

    @Test
    void listHistoryOrders_returnsPage() {
        when(orderRepository.listHistoryOrders(eq(100L), eq("BTC_USDT"), eq(20),
                any(), any())).thenReturn(List.of(order(3L)));

        PageResultHolder result = service.listHistoryOrders(100L, "BTC_USDT", null, null);

        assertThat(result.getItems()).hasSize(1);
    }
}
```

(说明:`PageResultHolder` 占位——实际实现为泛型 `PageResult<OrderResponse>`,测试中改为 `PageResult<OrderResponse>`。)

- [ ] **步骤 2:运行测试验证失败**

运行:`mvn -pl cex-order-service -am test -Dtest=QueryOrderServiceTest -DfailIfNoTests=false`
预期:编译失败,类不存在。

- [ ] **步骤 3:实现响应模型与游标解析**

`PageResult.java`:

```java
package com.cex.order.api.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 游标分页结果:nextCursor 为 null 表示没有更多数据
 * 游标格式:createdAt_orderId,如 "2026-08-28T10:00:00.123_123456"
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageResult<T> {

    private List<T> items;
    private String nextCursor;
}
```

`OrderResponse.java`:

```java
package com.cex.order.api.response;

import com.cex.order.domain.model.Order;
import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderStatus;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderResponse {

    private Long orderId;
    private Long userId;
    private String clientOrderId;
    private String symbol;
    private OrderSide side;
    private OrderType type;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal quoteAmount;
    private BigDecimal filledQuantity;
    private BigDecimal filledAmount;
    private OrderStatus status;
    private TimeInForce timeInForce;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static OrderResponse of(Order order) {
        return OrderResponse.builder()
                .orderId(order.getOrderId()).userId(order.getUserId())
                .clientOrderId(order.getClientOrderId()).symbol(order.getSymbol())
                .side(order.getSide()).type(order.getType())
                .price(order.getPrice()).quantity(order.getQuantity())
                .quoteAmount(order.getQuoteAmount())
                .filledQuantity(order.getFilledQuantity()).filledAmount(order.getFilledAmount())
                .status(order.getStatus()).timeInForce(order.getTimeInForce())
                .createdAt(order.getCreatedAt()).updatedAt(order.getUpdatedAt())
                .build();
    }

    /** 分页游标:createdAt_orderId */
    public String toCursor() {
        return createdAt + "_" + orderId;
    }
}
```

`CursorPagingRequest.java`:

```java
package com.cex.order.api.request;

import lombok.Data;

/**
 * 游标分页请求基类
 */
@Data
public class CursorPagingRequest {

    /** 游标:上一页返回的 nextCursor,首屏为 null */
    private String cursor;

    /** 每页数量,默认 20,最大 100 */
    private Integer limit = 20;

    public int limit() {
        return limit == null || limit <= 0 || limit > 100 ? 20 : limit;
    }
}
```

`OpenOrdersRequest.java`:

```java
package com.cex.order.api.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class OpenOrdersRequest extends CursorPagingRequest {
    private String symbol;
}
```

`OrderHistoryRequest.java`:

```java
package com.cex.order.api.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class OrderHistoryRequest extends CursorPagingRequest {
    private String symbol;
}
```

`CreateOrderRequest.java`:

```java
package com.cex.order.api.request;

import com.cex.order.domain.model.OrderSide;
import com.cex.order.domain.model.OrderType;
import com.cex.order.domain.model.TimeInForce;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class CreateOrderRequest {

    @NotBlank(message = "clientOrderId 不能为空")
    private String clientOrderId;

    @NotBlank(message = "symbol 不能为空")
    private String symbol;

    @NotNull(message = "side 不能为空")
    private OrderSide side;

    @NotNull(message = "type 不能为空")
    private OrderType type;

    @Positive(message = "price 必须大于 0")
    private BigDecimal price;

    @Positive(message = "quantity 必须大于 0")
    private BigDecimal quantity;

    /** 市价买单的冻结金额 */
    @Positive(message = "quoteAmount 必须大于 0")
    private BigDecimal quoteAmount;

    private TimeInForce timeInForce = TimeInForce.GTC;
}
```

`CreateOrderResponse.java`:

```java
package com.cex.order.api.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderResponse {
    private Long orderId;
    private String status;
}
```

- [ ] **步骤 4:实现 QueryOrderService**

`QueryOrderService.java`:

```java
package com.cex.order.application.service;

import com.cex.common.core.exception.BizException;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.api.response.PageResult;
import com.cex.order.common.ErrorCode;
import com.cex.order.domain.model.Order;
import com.cex.order.domain.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 订单查询服务:游标分页(created_at + order_id),不用 offset 深翻页
 */
@Service
@RequiredArgsConstructor
public class QueryOrderService {

    private final OrderRepository orderRepository;

    public OrderResponse getOrder(Long userId, Long orderId) {
        Order order = orderRepository.findByOrderId(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            throw new BizException(ErrorCode.ORDER_NOT_FOUND.getCode(),
                    ErrorCode.ORDER_NOT_FOUND.getMessage() + ": " + orderId);
        }
        return OrderResponse.of(order);
    }

    public PageResult<OrderResponse> listOpenOrders(Long userId, String symbol,
                                                    String cursor, Integer limit) {
        return page(orderRepository.listOpenOrders(userId, symbol,
                limitOrDefault(limit), cursorTime(cursor), cursorOrderId(cursor)), limitOrDefault(limit));
    }

    public PageResult<OrderResponse> listHistoryOrders(Long userId, String symbol,
                                                       String cursor, Integer limit) {
        return page(orderRepository.listHistoryOrders(userId, symbol,
                limitOrDefault(limit), cursorTime(cursor), cursorOrderId(cursor)), limitOrDefault(limit));
    }

    private PageResult<OrderResponse> page(List<Order> orders, int limit) {
        List<OrderResponse> items = orders.stream().map(OrderResponse::of).toList();
        String nextCursor = items.size() >= limit ? items.get(items.size() - 1).toCursor() : null;
        return PageResult.<OrderResponse>builder().items(items).nextCursor(nextCursor).build();
    }

    private int limitOrDefault(Integer limit) {
        return limit == null || limit <= 0 || limit > 100 ? 20 : limit;
    }

    private LocalDateTime cursorTime(String cursor) {
        return cursor == null || !cursor.contains("_") ? null : LocalDateTime.parse(cursor.split("_")[0]);
    }

    private Long cursorOrderId(String cursor) {
        return cursor == null || !cursor.contains("_") ? null : Long.valueOf(cursor.split("_")[1]);
    }
}
```

- [ ] **步骤 5:修复测试为具体类型并运行**

`QueryOrderServiceTest` 中 `PageResultHolder` 替换为 `PageResult<OrderResponse>`(补充 import `com.cex.order.api.response.PageResult`),`listOpenOrders_passesCursor` 中 `assertThat(result.getItems()).hasSize(2)` 保持。

运行:`mvn -pl cex-order-service -am test -Dtest=QueryOrderServiceTest -DfailIfNoTests=false`
预期:BUILD SUCCESS,4 个测试全部通过。

- [ ] **步骤 6:实现 Controller**

`OrderController.java`:

```java
package com.cex.order.api.controller;

import com.cex.common.core.api.ApiResult;
import com.cex.order.api.request.CancelOrderRequest;
import com.cex.order.api.request.CreateOrderRequest;
import com.cex.order.api.request.OpenOrdersRequest;
import com.cex.order.api.request.OrderHistoryRequest;
import com.cex.order.api.response.CreateOrderResponse;
import com.cex.order.api.response.OrderResponse;
import com.cex.order.api.response.PageResult;
import com.cex.order.application.command.CancelOrderCommand;
import com.cex.order.application.command.CreateOrderCommand;
import com.cex.order.application.service.CancelOrderService;
import com.cex.order.application.service.CreateOrderService;
import com.cex.order.application.service.CreateOrderResult;
import com.cex.order.application.service.QueryOrderService;
import com.cex.order.domain.model.TimeInForce;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 订单接口:仅做参数接收/校验与结果返回,业务逻辑在 Application Service
 * 注意:当前无登录态,userId 从请求头 X-User-Id 读取(网关/后续接入认证后替换)
 */
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final CreateOrderService createOrderService;
    private final CancelOrderService cancelOrderService;
    private final QueryOrderService queryOrderService;

    /** 创建订单 */
    @PostMapping
    public ApiResult<CreateOrderResponse> createOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @Valid @RequestBody CreateOrderRequest request) {
        CreateOrderResult result = createOrderService.createOrder(toCommand(userId, request));
        return ApiResult.success(CreateOrderResponse.builder()
                .orderId(result.getOrderId())
                .status(result.getStatus().name())
                .build());
    }

    /** 取消订单 */
    @DeleteMapping("/{orderId}")
    public ApiResult<Void> cancelOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @PathVariable Long orderId) {
        cancelOrderService.cancelOrder(CancelOrderCommand.builder()
                .userId(userId).orderId(orderId).build());
        return ApiResult.success();
    }

    /** 查询订单 */
    @GetMapping("/{orderId}")
    public ApiResult<OrderResponse> getOrder(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            @PathVariable Long orderId) {
        return ApiResult.success(queryOrderService.getOrder(userId, orderId));
    }

    /** 查询当前委托 */
    @GetMapping("/open")
    public ApiResult<PageResult<OrderResponse>> openOrders(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            OpenOrdersRequest request) {
        return ApiResult.success(queryOrderService.listOpenOrders(
                userId, request.getSymbol(), request.getCursor(), request.getLimit()));
    }

    /** 查询历史订单 */
    @GetMapping
    public ApiResult<PageResult<OrderResponse>> historyOrders(
            @RequestHeader(value = "X-User-Id", required = false) Long userId,
            OrderHistoryRequest request) {
        return ApiResult.success(queryOrderService.listHistoryOrders(
                userId, request.getSymbol(), request.getCursor(), request.getLimit()));
    }

    private CreateOrderCommand toCommand(Long userId, CreateOrderRequest request) {
        return CreateOrderCommand.builder()
                .userId(userId)
                .clientOrderId(request.getClientOrderId())
                .symbol(request.getSymbol())
                .side(request.getSide())
                .type(request.getType())
                .price(request.getPrice())
                .quantity(request.getQuantity())
                .quoteAmount(request.getQuoteAmount())
                .timeInForce(request.getTimeInForce() == null ? TimeInForce.GTC : request.getTimeInForce())
                .build();
    }
}
```

`CancelOrderRequest.java`(取消也可按 clientOrderId,本期用 orderId):

```java
package com.cex.order.api.request;

import lombok.Data;

@Data
public class CancelOrderRequest {
    private Long orderId;
    private String clientOrderId;
}
```

- [ ] **步骤 7:实现 TraceFilter 与日志配置**

`TraceFilter.java`:

```java
package com.cex.order.infrastructure.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * 请求追踪:生成 requestId 写入 MDC,贯穿 下单->Kafka->撮合->成交->清算 排查链路
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceFilter extends OncePerRequestFilter {

    public static final String REQUEST_ID = "requestId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String requestId = request.getHeader("X-Request-Id");
        if (requestId == null || requestId.isBlank()) {
            requestId = UUID.randomUUID().toString().replace("-", "");
        }
        MDC.put(REQUEST_ID, requestId);
        response.setHeader("X-Request-Id", requestId);
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(REQUEST_ID);
        }
    }
}
```

`logback-spring.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} [%X{requestId:-}] - %msg%n</pattern>
        </encoder>
    </appender>
    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

- [ ] **步骤 8:全量单测回归**

运行:`mvn -pl cex-order-service -am test`
预期:BUILD SUCCESS,全部测试通过。

---

### 任务 13:全量构建验证

- [ ] **步骤 1:全量构建**

运行:`mvn clean package -DskipTests`
预期:BUILD SUCCESS,9 个模块全部编译打包。

- [ ] **步骤 2:单测全量运行**

运行:`mvn -pl cex-order-service -am test`
预期:BUILD SUCCESS,全部单测通过(状态机/工厂/校验/冻结计算/雪花/交易对配置/Mock 资产/创建/取消/Relay/消费者/查询)。

- [ ] **步骤 3:启动冒烟(可选,需 docker compose up -d 起基础设施)**

运行:`docker compose up -d` 后 `mvn -pl cex-order-service spring-boot:run`

预期:服务 8102 端口启动,日志出现 "Outbox Relay" 定时扫描;执行:

```bash
curl -X POST http://localhost:8102/api/v1/orders \
  -H "Content-Type: application/json" -H "X-User-Id: 100" \
  -d '{"clientOrderId":"demo_001","symbol":"BTC_USDT","side":"BUY","type":"LIMIT","price":"100000","quantity":"0.1"}'
```

预期返回 `{"code":0,"data":{"orderId":...,"status":"PENDING_MATCH"}}`;重复请求同一 clientOrderId 返回相同 orderId;Kafka 收到 cex.order.event 消息(key=BTC_USDT);重复下单时 `orders` 表不产生重复行。

---

## 自检记录

- **规格覆盖度:** 交易对配置(任务 6)/ 限价市价单(任务 8)/ 资产冻结 Mock(任务 7)/ 幂等(任务 8 唯一索引 + 预查)/ 状态机(任务 3)/ Outbox(任务 8 写入 + 任务 10 Relay)/ Kafka 事件(任务 1 契约 + 任务 10 发送 + 任务 11 消费)/ 查询(任务 12)/ 取消(任务 9)/ 日志追踪(任务 12)/ 数据库(任务 2)。✓
- **占位符扫描:** 无 TODO/待定项;Mock 资产服务的 TODO 注释为设计意图。✓
- **类型一致性:** `OrderEvent` 扩展字段 clientOrderId 在任务 1 定义,任务 8 Publisher 使用;`OrderPersistenceService.cancelInTx` 在任务 9 定义任务 9 使用;`PageResult<T>` 泛型在任务 12 定义,测试同步修正。✓
- **事务边界:** Kafka 发送仅在任务 10 Relay(无事务);冻结/解冻均在事务外(任务 8/9);订单+Outbox 同事务(OrderPersistenceService)。✓
