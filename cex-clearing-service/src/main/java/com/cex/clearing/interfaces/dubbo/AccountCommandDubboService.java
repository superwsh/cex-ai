package com.cex.clearing.interfaces.dubbo;

import com.cex.clearing.application.service.AccountCommandApplicationService;
import com.cex.common.dubbo.account.AccountCommandService;
import com.cex.common.dubbo.account.FreezeBalanceRequest;
import com.cex.common.dubbo.account.UnfreezeBalanceRequest;
import lombok.RequiredArgsConstructor;
import org.apache.dubbo.config.annotation.DubboService;

/** 清算服务对订单服务暴露的账户资金命令实现。 */
@DubboService
@RequiredArgsConstructor
public class AccountCommandDubboService implements AccountCommandService {

    private final AccountCommandApplicationService applicationService;

    @Override
    public void freeze(FreezeBalanceRequest request) {
        applicationService.freeze(request);
    }

    @Override
    public void unfreeze(UnfreezeBalanceRequest request) {
        applicationService.unfreeze(request);
    }
}
