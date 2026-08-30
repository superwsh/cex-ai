package com.cex.matching.application;

import com.cex.matching.domain.model.CommandType;
import com.cex.matching.domain.model.MatchOrder;
import com.cex.matching.domain.model.MatchingCommand;

import java.math.BigDecimal;
import java.util.Map;

/** 将外部撮合命令转换为引擎内部订单。 */
public final class CommandNormalizer {

    private final Map<String, DecimalScale> scales;

    public CommandNormalizer(Map<String, DecimalScale> scales) {
        this.scales = Map.copyOf(scales);
    }

    public MatchOrder toMatchOrder(MatchingCommand command) {
        DecimalScale scale = scales.get(command.symbol());
        if (scale == null || command.commandType() != CommandType.NEW_ORDER) {
            throw new IllegalArgumentException("命令不能归一化为订单");
        }
        try {
            return new MatchOrder(
                    Long.parseLong(command.orderId()), Long.parseLong(command.userId()),
                    command.symbol(), command.side(),
                    BigDecimal.valueOf(command.price(), scale.priceScale()),
                    BigDecimal.valueOf(command.quantity(), scale.quantityScale()),
                    BigDecimal.valueOf(command.quantity(), scale.quantityScale()),
                    command.sequence());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("命令数值身份不能转换为 long", exception);
        }
    }
}
