package com.cex.market.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 订单簿恢复快照持久化对象。 */
@Data
@TableName("market_order_book_snapshot")
public class MarketOrderBookSnapshotPO {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String symbol;
    private Long snapshotSequence;
    private String bidsJson;
    private String asksJson;
    private Integer kafkaPartition;
    private Long kafkaOffset;
    private Long snapshotTime;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
