package com.cex.order.infrastructure.asset;

import com.cex.common.dubbo.account.AccountCommandService;
import com.cex.common.dubbo.account.FreezeBalanceRequest;
import com.cex.common.dubbo.account.UnfreezeBalanceRequest;
import org.apache.dubbo.config.annotation.DubboReference;
import org.springframework.stereotype.Component;

/** 通过 Dubbo 调用清算服务的持久化账户冻结能力。 */
@Component
public class ClearingAccountServiceClient implements AccountServiceClient {

    @DubboReference(check = false)
    private AccountCommandService accountCommandService;

    @Override
    public void freeze(FreezeRequest request) {
        accountCommandService.freeze(new FreezeBalanceRequest(request.getUserId(), request.getCurrency(),
                request.getAmount(), request.getBizType(), String.valueOf(request.getBizId())));
    }

    @Override
    public void unfreeze(UnfreezeRequest request) {
        accountCommandService.unfreeze(new UnfreezeBalanceRequest(request.getUserId(), request.getCurrency(),
                request.getAmount(), request.getBizType(), String.valueOf(request.getBizId())));
    }
}
