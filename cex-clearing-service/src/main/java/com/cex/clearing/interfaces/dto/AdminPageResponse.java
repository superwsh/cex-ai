package com.cex.clearing.interfaces.dto;

import java.util.List;

/** 后台查询分页结果。 */
public record AdminPageResponse<T>(List<T> items, long total, long pageNo, long pageSize) {
}
