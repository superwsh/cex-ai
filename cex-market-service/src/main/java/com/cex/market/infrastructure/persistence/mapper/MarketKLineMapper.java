package com.cex.market.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.market.infrastructure.persistence.entity.MarketKLinePO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/** KLine 历史数据 Mapper。 */
@Mapper
public interface MarketKLineMapper extends BaseMapper<MarketKLinePO> {

    /** 利用唯一索引幂等写入已收线 KLine。 */
    @Insert("""
            INSERT INTO market_kline (symbol, `interval`, open_time, close_time, open_price, high_price,
                low_price, close_price, volume, quote_volume, trade_count)
            VALUES (#{symbol}, #{interval}, #{openTime}, #{closeTime}, #{openPrice}, #{highPrice},
                #{lowPrice}, #{closePrice}, #{volume}, #{quoteVolume}, #{tradeCount})
            ON DUPLICATE KEY UPDATE close_time = VALUES(close_time), open_price = VALUES(open_price),
                high_price = VALUES(high_price), low_price = VALUES(low_price), close_price = VALUES(close_price),
                volume = VALUES(volume), quote_volume = VALUES(quote_volume), trade_count = VALUES(trade_count)
            """)
    int upsertClosed(MarketKLinePO kLine);

    /** 查询指定时间范围内的已收线 KLine。 */
    @Select("""
            <script>
            SELECT id, symbol, `interval`, open_time, close_time, open_price, high_price, low_price,
                   close_price, volume, quote_volume, trade_count, created_at, updated_at
            FROM market_kline
            WHERE symbol = #{symbol} AND `interval` = #{interval}
            <if test='startTime != null'> AND open_time &gt;= #{startTime}</if>
            <if test='endTime != null'> AND open_time &lt;= #{endTime}</if>
            ORDER BY open_time ASC
            LIMIT #{limit}
            </script>
            """)
    List<MarketKLinePO> findClosed(@Param("symbol") String symbol, @Param("interval") String interval,
                                   @Param("startTime") Long startTime, @Param("endTime") Long endTime,
                                   @Param("limit") int limit);
}
