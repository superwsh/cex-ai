package com.cex.order.infrastructure.persistence.mapper;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/** 撮合命令序号的数据访问接口。 */
@Mapper
public interface MatchingCommandSequenceMapper {

    /**
     * 在数据库内为一个交易对原子分配下一个命令序号。
     *
     * @param symbol 交易对
     */
    @Insert("""
            INSERT INTO matching_command_sequence (symbol, last_sequence, updated_at)
            VALUES (#{symbol}, LAST_INSERT_ID(1), NOW())
            ON DUPLICATE KEY UPDATE
                last_sequence = LAST_INSERT_ID(last_sequence + 1),
                updated_at = NOW()
            """)
    void allocateNext(@Param("symbol") String symbol);

    /**
     * 读取当前连接刚刚分配的撮合命令序号。
     *
     * @return 当前会话的最后分配序号
     */
    @Select("SELECT LAST_INSERT_ID()")
    Long selectLastAllocatedSequence();
}
