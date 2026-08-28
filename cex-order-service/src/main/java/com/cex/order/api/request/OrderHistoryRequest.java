package com.cex.order.api.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class OrderHistoryRequest extends CursorPagingRequest {
    private String symbol;
}
