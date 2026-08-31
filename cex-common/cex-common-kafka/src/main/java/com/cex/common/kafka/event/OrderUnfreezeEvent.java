package com.cex.common.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/** 订单终态后释放剩余冻结资金的幂等指令。 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderUnfreezeEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private Long orderId;
    private Long userId;
    private String asset;
    private BigDecimal amount;
    private String reason;
    private Long timestamp;
}
