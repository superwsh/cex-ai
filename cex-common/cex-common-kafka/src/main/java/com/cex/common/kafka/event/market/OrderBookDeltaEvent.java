package com.cex.common.kafka.event.market;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 撮合引擎输出的某交易对 Level 2 盘口增量。
 * 同一 symbol 的事件必须按 sequence 连续投递；previousSequence 用于消费者检测缺口。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderBookDeltaEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 事件全局唯一标识，供下游幂等记录使用。 */
    private String eventId;

    /** 事件协议版本。缺失时按版本 1 处理。 */
    @Builder.Default
    private Integer eventVersion = 1;

    /** 交易对。 */
    private String symbol;

    /** 当前变更后的交易对内盘口序号。 */
    private long sequence;

    /** 当前事件所基于的上一盘口序号。 */
    private long previousSequence;

    /**
     * 触发本次盘口变化的撮合命令序号，仅用于追溯；不参与盘口增量连续性判断。
     */
    private Long sourceSequence;

    /** 买方价格档位变更。 */
    @Builder.Default
    private List<PriceLevelChange> bids = List.of();

    /** 卖方价格档位变更。 */
    @Builder.Default
    private List<PriceLevelChange> asks = List.of();

    /** 盘口变更发生时间（毫秒时间戳）。 */
    private long eventTime;
}
