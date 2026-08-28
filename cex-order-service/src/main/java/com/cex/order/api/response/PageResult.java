package com.cex.order.api.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 游标分页结果:nextCursor 为 null 表示没有更多数据
 * 游标格式:createdAt_orderId,如 "2026-08-28T10:00:00.123_123456"
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PageResult<T> {

    private List<T> items;
    private String nextCursor;
}
