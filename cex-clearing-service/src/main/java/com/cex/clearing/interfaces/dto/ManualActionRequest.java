package com.cex.clearing.interfaces.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;

/** 清算后台人工操作请求。 */
@Data
public class ManualActionRequest {
    @NotBlank
    @Size(max = 256)
    private String reason;
}
