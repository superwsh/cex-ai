# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 构建与运行

- 构建环境要求：**JDK 17 + Maven 3.6.3+**（Spring Boot 3 不支持旧版）
- 全量构建：`mvn clean package -DskipTests`
- 启动单个服务（先 `docker compose up -d` 启动基础设施）：`mvn -pl cex-user-service spring-boot:run`
- 依赖 IDE 编译时确保 IDE 使用 JDK 17（本机系统 JAVA_HOME 可能是 JDK 8，勿以 `java -version` 判断项目环境）
- 基础设施全部由 `docker compose up -d` 提供：MySQL(3306)/Redis(6379)/Kafka(9092)/Nacos(8848)/ES(9200)/Prometheus(9090)/Grafana(3000)/Jaeger(16686)

## 架构总览

事件驱动的微服务架构（详见 [架构设计.md](docs/架构设计.md) 与 [技术选型.md](docs/技术选型.md)）：

```
订单链路: Order Service(本地事务+Outbox) -> Kafka[cex.order.event] -> Matching Engine
         -> Kafka[cex.trade.event] -> Clearing(Mysql:cex_account) / Market(Netty WS 9001) / Notification(Netty WS 9002)
```

- **端口映射**：gateway 8080，user 8101，order 8102，asset 8103，matching 8104，clearing 8105，market 8106，notification 8107
- **数据库按服务分库**：cex_user / cex_order / cex_asset / cex_account，初始化脚本在 [docker/mysql/init](docker/mysql/init/01-databases.sql)
- **撮合引擎**（[cex-matching-engine](cex-matching-engine/)）：无数据库，内存订单簿，按 symbol 分区 + 单线程消费保证顺序（`enable-auto-commit: false`）

## 关键约定（改动时务必遵守）

1. **版本号只能改根 [pom.xml](pom.xml)**：子模块依赖一律不写版本号，全部从根 POM 的 BOM（Spring Boot 3.3.5 / Spring Cloud 2023.0.3 / SCA 2023.0.1.0）继承。升级版本必须保持该兼容矩阵，勿单独升级某一组件
2. **服务启动类必须用 `@SpringBootApplication(scanBasePackages = "com.cex")`**：公共模块的配置类（RedisConfig、MybatisPlusConfig、GlobalExceptionHandler 等）靠此扫描生效，没有 spring.factories
3. **RPC 用 Dubbo**：Provider/Consumer 服务启动类需加 `@EnableDubbo`，协议 tri，注册中心 `nacos://`，配置在各自 application.yml
4. **Kafka 事件统一 JSON 序列化**：consumer 需配 `spring.json.trusted.packages: "com.cex.*"`；事件模型定义在 [cex-common-kafka](cex-common/cex-common-kafka/src/main/java/com/cex/common/kafka/event/)，新事件必须加在这里供所有服务共享
5. **网关是 WebFlux**：[cex-gateway](cex-gateway/) 不得引入 `spring-boot-starter-web` 或任何 common-web 依赖（servlet 栈会冲突）
6. **ShardingSphere 分库分表默认关闭**：启用需同时取消 common-mysql 与服务 pom 中的注释依赖并配置 `spring.shardingsphere.*`（yml 有示例），勿默认开启以免启动失败
7. **本地开发配置优先**：各服务 application.yml 使用本地直连配置，Nacos 配置中心为可选接入（`spring.config.import` 被注释）
