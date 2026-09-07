# `user-service/AGENTS.md`

## 1. 模块性质

Account Service 属于核心资金模块。

资金安全优先级最高。

任何修改必须优先分析：

- 是否可能重复扣款
- 是否可能重复入账
- 是否可能产生负余额
- 是否可能余额更新成功但流水失败
- 是否可能流水成功但余额失败
- 是否支持审计
- 是否支持对账

---

## 2. 余额模型

典型余额至少包含：

```text
available
frozen
```

禁止无业务依据出现：

```text
available < 0
frozen < 0
```

---

## 3. 冻结资金

标准语义：

```text
available -= amount
frozen += amount
```

应通过原子条件 UPDATE 等方式防止超扣。

示例：

```sql
UPDATE account_balance
SET available = available - :amount,
    frozen = frozen + :amount
WHERE user_id = :userId
  AND asset = :asset
  AND available >= :amount;
```

必须检查 affected rows。

---

## 4. 解冻资金

典型语义：

```text
frozen -= amount
available += amount
```

必须保证：

```text
frozen >= amount
```

不得让 frozen 变成负数。

---

## 5. 资金扣减

禁止：

```text
SELECT available
↓
Java 判断
↓
UPDATE
```

应使用数据库条件更新或项目中已经验证的等价并发控制方案。

---

## 6. 资金流水

每次核心余额变化必须可追踪。

流水建议包含：

```text
flowId
userId
asset
bizId
bizType

availableBefore
availableChange
availableAfter

frozenBefore
frozenChange
frozenAfter

createdAt
```

至少保证能够回答：

> 为什么这个账户余额变成现在这个值？

---

## 7. 流水幂等

典型成交场景可以使用：

```text
tradeId + userId + bizType
```

建立唯一索引。

其他业务使用对应稳定业务 ID。

禁止单纯通过：

```text
SELECT count(*) → INSERT
```

作为最终并发幂等方案。

---

## 8. 余额与流水事务

余额修改与对应资金流水如果处于同一数据库，应在同一本地事务中。

例如：

```text
BEGIN

UPDATE account_balance

INSERT fund_flow

COMMIT
```

任何一步失败：

```text
ROLLBACK
```

---

## 9. 并发

必须重点检查：

```text
同一账户
+
同一币种
+
并发资金变化
```

不得依赖 JVM 单机锁作为最终金融正确性保证，除非架构明确确保该账户永久由单实例单线程处理。

---

## 10. Retry

资金操作 Retry 必须是幂等的。

发生：

```text
DB timeout
```

不能假设：

```text
UPDATE 没有成功
```

必须先根据业务 ID / 流水状态判断当前实际状态。

---

## 11. 测试

Account 核心修改至少根据范围覆盖：

```text
余额充足
余额不足
余额刚好
冻结
解冻
扣减
入账
重复请求
重复流水
并发扣减
事务回滚
数据库异常
```

并发扣款必须验证：

```text
不会负余额
不会超扣
不会重复流水
最终余额正确
```