package com.cex.market.infrastructure.persistence.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/** 已收线 KLine 持久化对象。 */
@Data
@TableName("market_kline")
public class MarketKLinePO {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String symbol;
    private String interval;
    private Long openTime;
    private Long closeTime;
    private BigDecimal openPrice;
    private BigDecimal highPrice;
    private BigDecimal lowPrice;
    private BigDecimal closePrice;
    private BigDecimal volume;
    private BigDecimal quoteVolume;
    private Long tradeCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
