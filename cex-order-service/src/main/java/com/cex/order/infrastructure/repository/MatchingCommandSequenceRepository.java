package com.cex.order.infrastructure.repository;

import com.cex.order.infrastructure.persistence.mapper.MatchingCommandSequenceMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** 为同一交易对分配严格连续的撮合命令序号。 */
@Repository
@RequiredArgsConstructor
public class MatchingCommandSequenceRepository {

    private final MatchingCommandSequenceMapper sequenceMapper;

    /**
     * 在调用方事务内原子分配下一个交易对命令序号。
     *
     * @param symbol 交易对
     * @return 从一开始、严格递增的交易对内序号
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public long allocateNext(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            throw new IllegalArgumentException("交易对不能为空");
        }
        sequenceMapper.allocateNext(symbol);
        Long sequence = sequenceMapper.selectLastAllocatedSequence();
        if (sequence == null || sequence <= 0) {
            throw new IllegalStateException("撮合命令序号分配失败: symbol=" + symbol);
        }
        return sequence;
    }
}
