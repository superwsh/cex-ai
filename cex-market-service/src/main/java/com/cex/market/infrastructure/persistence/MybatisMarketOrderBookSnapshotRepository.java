package com.cex.market.infrastructure.persistence;

import com.cex.market.application.service.MarketOrderBookSnapshotRepository;
import com.cex.market.domain.model.MarketOrderBookSnapshot;
import com.cex.market.domain.model.MarketPriceLevel;
import com.cex.market.infrastructure.persistence.entity.MarketOrderBookSnapshotPO;
import com.cex.market.infrastructure.persistence.mapper.MarketOrderBookSnapshotMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;

/** MyBatis 订单簿恢复快照仓储实现。 */
@Repository
@RequiredArgsConstructor
public class MybatisMarketOrderBookSnapshotRepository implements MarketOrderBookSnapshotRepository {

    private static final TypeReference<List<MarketPriceLevel>> PRICE_LEVEL_LIST = new TypeReference<>() {
    };

    private final MarketOrderBookSnapshotMapper snapshotMapper;
    private final ObjectMapper objectMapper;

    /**
     * 以交易对为幂等键保存最新的订单簿恢复快照。
     *
     * @param snapshot 最新完整订单簿快照
     */
    @Override
    public void save(MarketOrderBookSnapshot snapshot) {
        snapshotMapper.upsert(toPO(snapshot));
    }

    /**
     * 查询指定交易对的恢复快照。
     *
     * @param symbol 交易对
     * @return 快照；不存在时为空
     */
    @Override
    public MarketOrderBookSnapshot findBySymbol(String symbol) {
        MarketOrderBookSnapshotPO snapshot = snapshotMapper.findBySymbol(symbol);
        return snapshot == null ? null : toDomain(snapshot);
    }

    /**
     * 查询全部交易对恢复快照。
     *
     * @return 快照列表
     */
    @Override
    public List<MarketOrderBookSnapshot> findAll() {
        return snapshotMapper.findAllSnapshots().stream().map(this::toDomain).toList();
    }

    /**
     * 将领域快照转为持久化对象。
     *
     * @param snapshot 领域快照
     * @return 持久化对象
     */
    private MarketOrderBookSnapshotPO toPO(MarketOrderBookSnapshot snapshot) {
        MarketOrderBookSnapshotPO po = new MarketOrderBookSnapshotPO();
        po.setSymbol(snapshot.symbol());
        po.setSnapshotSequence(snapshot.sequence());
        po.setBidsJson(writeLevels(snapshot.bids()));
        po.setAsksJson(writeLevels(snapshot.asks()));
        po.setKafkaPartition(snapshot.kafkaPartition());
        po.setKafkaOffset(snapshot.kafkaOffset());
        po.setSnapshotTime(snapshot.createdAt());
        return po;
    }

    /**
     * 将持久化对象转为领域快照。
     *
     * @param po 持久化对象
     * @return 领域快照
     */
    private MarketOrderBookSnapshot toDomain(MarketOrderBookSnapshotPO po) {
        return new MarketOrderBookSnapshot(po.getSymbol(), po.getSnapshotSequence(), readLevels(po.getBidsJson()),
                readLevels(po.getAsksJson()), po.getKafkaPartition(), po.getKafkaOffset(), po.getSnapshotTime());
    }

    /**
     * 序列化价格档位集合。
     *
     * @param levels 价格档位
     * @return JSON 文本
     */
    private String writeLevels(List<MarketPriceLevel> levels) {
        try {
            return objectMapper.writeValueAsString(levels);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("订单簿快照序列化失败", exception);
        }
    }

    /**
     * 反序列化价格档位集合。
     *
     * @param levelsJson JSON 文本
     * @return 价格档位
     */
    private List<MarketPriceLevel> readLevels(String levelsJson) {
        try {
            return objectMapper.readValue(levelsJson, PRICE_LEVEL_LIST);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("订单簿快照反序列化失败", exception);
        }
    }
}
