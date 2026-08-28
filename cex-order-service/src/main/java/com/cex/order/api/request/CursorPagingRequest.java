package com.cex.order.api.request;

import lombok.Data;

/**
 * 游标分页请求基类
 */
@Data
public class CursorPagingRequest {

    /** 游标:上一页返回的 nextCursor,首屏为 null */
    private String cursor;

    /** 每页数量,默认 20,最大 100 */
    private Integer limit = 20;

    public int limit() {
        return limit == null || limit <= 0 || limit > 100 ? 20 : limit;
    }
}
