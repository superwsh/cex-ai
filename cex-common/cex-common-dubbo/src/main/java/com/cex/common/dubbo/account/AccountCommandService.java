package com.cex.common.dubbo.account;

/**
 * 账户资金命令接口。
 * 调用方只能冻结或解冻，不能通过接口直接设置用户余额。
 */
public interface AccountCommandService {

    /** 冻结可用余额。 */
    void freeze(FreezeBalanceRequest request);

    /** 将冻结余额解冻至可用余额。 */
    void unfreeze(UnfreezeBalanceRequest request);
}
