# CEX AI — Web3 中心化交易所微服务平台

基于 [技术选型.md](docs/技术选型.md) 与 [架构设计.md](docs/架构设计.md) 初始化的微服务工程骨架。

## 项目结构

```
cex-ai/
├── pom.xml                      # 父 POM：统一版本管理（版本兼容矩阵见下表）
├── docker-compose.yml           # 基础设施编排（MySQL/Redis/Kafka/Nacos/ES/Prometheus/Grafana/Jaeger）
├── docker/                      # Prometheus 配置、MySQL 初始化脚本
├── cex-common/
│   ├── cex-common-core/         # 统一返回结构、业务异常、常量（零依赖）
│   ├── cex-common-web/          # Jackson 序列化、全局异常、Nacos 注册、监控
│   ├── cex-common-redis/        # RedisTemplate 统一序列化（Lettuce）
│   ├── cex-common-mysql/        # MyBatis-Plus 配置（分页/乐观锁）
│   ├── cex-common-kafka/        # Topic 常量、事件模型（OrderEvent/TradeEvent）
│   └── cex-common-dubbo/        # Dubbo 框架集成（Triple 协议 + Nacos 注册）
├── cex-gateway/                 # API 网关（Spring Cloud Gateway，端口 8080）
├── cex-user-service/            # 用户服务（8101）
├── cex-order-service/           # 订单服务（8102，Outbox 模式发布事件）
├── cex-asset-service/           # 资产服务（8103）
├── cex-matching-engine/         # 撮合引擎（8104，内存订单簿，无数据库）
├── cex-clearing-service/        # 清算服务（8105，Account/Ledger）
├── cex-market-service/          # 行情服务（8106，Netty WebSocket 9001）
└── cex-notification-service/    # 通知服务（8107，Netty WebSocket 9002）
```

## 版本兼容矩阵

> 组合经官方兼容矩阵验证：Spring Boot 3.3.x ↔ Spring Cloud 2023.0.x (Leyton) ↔ Spring Cloud Alibaba 2023.0.1.0。

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| Java | 17 | 技术选型要求 |
| Spring Boot | 3.3.5 | 父 POM parent，BOM 管理全部 Spring 依赖 |
| Spring Cloud | 2023.0.3 (Leyton) | 与 Spring Boot 3.3.x 官方兼容 |
| Spring Cloud Alibaba | 2023.0.1.0 | 内置 Nacos client 2.3.2（与服务端匹配） |
| Nacos (Server/Client) | 2.3.2 | 服务注册 + 配置中心 |
| Apache Dubbo | 3.3.2 | Triple 协议（HTTP/2），Nacos 注册中心 |
| Spring Cloud Gateway | 4.1.x（由 SC BOM 管理） | 网关路由，WebFlux 响应式 |
| Kafka (Server) | 3.7.0 | docker-compose，KRaft 模式（无 Zookeeper） |
| Kafka Client | 3.7.x（由 SB BOM 管理） | spring-kafka 集成 |
| MyBatis-Plus | 3.5.7 | mybatis-plus-spring-boot3-starter |
| ShardingSphere-JDBC | 5.5.0 | 分库分表，按需启用（见下） |
| MySQL (Server) | 8.0.39 | docker-compose，初始化脚本自动建库 |
| MySQL Connector/J | 8.3.0（由 SB BOM 管理） | |
| Redis (Server) | 7.2.5 | 单机模式（生产切换 Redis Cluster，yml 有示例） |
| Lettuce | 6.3.x（由 SB BOM 管理） | Spring Data Redis 客户端 |
| Netty | 4.1.x（由 SB BOM 管理） | 行情/通知 WebSocket 推送 |
| Elasticsearch | 8.13.4 | 搜索（选型备用，当前未接入代码） |
| Prometheus | 2.53.0 | 指标采集（/actuator/prometheus） |
| Grafana | 11.1.0 | 可视化 |
| Jaeger | 1.60 | 全链路追踪（OTLP） |
| OpenTelemetry Java Agent | 2.4.0 | 自动埋点，启动参数接入（见下） |

## 快速开始

```bash
# 1. 启动基础设施（MySQL/Redis/Kafka/Nacos/ES/Prometheus/Grafana/Jaeger）
docker compose up -d

# 2. 编译构建（要求 JDK 17 + Maven 3.6.3+）
mvn clean package -DskipTests

# 3. 启动服务（建议顺序：先 Nacos 就绪，再启动任意服务）
mvn -pl cex-gateway spring-boot:run
mvn -pl cex-user-service spring-boot:run
# ... 其余服务同理
```

- 网关入口：`http://localhost:8080/api/user/**`（自动路由到各服务，也可直接访问各服务端口）
- Nacos 控制台：`http://localhost:8848/nacos`（默认无认证，开发环境）
- Prometheus：`http://localhost:9090` | Grafana：`http://localhost:3000`（admin/admin）
- Jaeger UI：`http://localhost:16686` | Kibana：`http://localhost:5601`

### 链路追踪接入（OpenTelemetry）

```bash
# 服务启动时挂载 OTel Java Agent（下载链接见官网，版本 2.4.0）
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.service.name=cex-user-service \
     -Dotel.exporter.otlp.endpoint=http://127.0.0.1:4317 \
     -jar cex-user-service.jar
```

### 分库分表启用步骤（ShardingSphere-JDBC）

1. 取消 [cex-common-mysql/pom.xml](cex-common/cex-common-mysql/pom.xml) 中 `shardingsphere-jdbc-spring-boot-starter` 的注释
2. 取消 [cex-order-service/pom.xml](cex-order-service/pom.xml) 中对应依赖的注释
3. 按 [cex-order-service/application.yml](cex-order-service/src/main/resources/application.yml) 中的注释示例配置 `spring.shardingsphere.*` 分表规则

## 核心事件链路（对应架构设计）

```
下单 -> Order Service（本地事务 + Outbox）-> Kafka[cex.order.event]
     -> Matching Engine（Symbol 分区/单线程/内存订单簿）-> Kafka[cex.trade.event]
     -> Clearing Service（记账/Ledger）+ Market Service（行情/WebSocket）+ Notification Service（推送）
```

所有服务配置默认使用本地配置，Nacos 配置中心可选接入（yml 中已注释 `spring.config.import` 示例）。
