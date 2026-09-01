package com.cex.common.kafka.event.market;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 行情聚合盘口中一个价格档位的最新数量。
 * quantity 为零表示删除该价格档位；不得为负数。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceLevelChange implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 价格。沿用项目 BigDecimal 精度约束，禁止使用浮点数。 */
    private BigDecimal price;

    /** 聚合后的档位数量；零表示删除。 */
    private BigDecimal quantity;
}
