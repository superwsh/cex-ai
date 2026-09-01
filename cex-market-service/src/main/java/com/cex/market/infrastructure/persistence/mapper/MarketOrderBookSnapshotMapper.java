package com.cex.market.infrastructure.persistence.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.cex.market.infrastructure.persistence.entity.MarketOrderBookSnapshotPO;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/** 订单簿恢复快照 Mapper。 */
@Mapper
public interface MarketOrderBookSnapshotMapper extends BaseMapper<MarketOrderBookSnapshotPO> {

    /** 使用交易对唯一索引单调写入最新快照。 */
    @Insert("""
            INSERT INTO market_order_book_snapshot (symbol, snapshot_sequence, bids_json, asks_json, kafka_partition,
                kafka_offset, snapshot_time)
            VALUES (#{symbol}, #{snapshotSequence}, #{bidsJson}, #{asksJson}, #{kafkaPartition}, #{kafkaOffset},
                #{snapshotTime})
            ON DUPLICATE KEY UPDATE bids_json = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(bids_json), bids_json), asks_json = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(asks_json), asks_json), kafka_partition = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(kafka_partition), kafka_partition), kafka_offset = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(kafka_offset), kafka_offset), snapshot_time = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(snapshot_time), snapshot_time), snapshot_sequence = IF(VALUES(snapshot_sequence) >= snapshot_sequence,
                VALUES(snapshot_sequence), snapshot_sequence)
            """)
    int upsert(MarketOrderBookSnapshotPO snapshot);

    /** 查询一个交易对的恢复快照。 */
    @Select("""
            SELECT id, symbol, snapshot_sequence, bids_json, asks_json, kafka_partition, kafka_offset, snapshot_time,
                   created_at, updated_at
            FROM market_order_book_snapshot
            WHERE symbol = #{symbol}
            """)
    MarketOrderBookSnapshotPO findBySymbol(String symbol);

    /** 查询全部交易对的恢复快照。 */
    @Select("""
            SELECT id, symbol, snapshot_sequence, bids_json, asks_json, kafka_partition, kafka_offset, snapshot_time,
                   created_at, updated_at
            FROM market_order_book_snapshot
            ORDER BY symbol ASC
            """)
    List<MarketOrderBookSnapshotPO> findAllSnapshots();
}
