package com.cex.order.infrastructure.asset;

/**
 * 账户服务客户端:资产冻结/解冻
 * 冻结接口幂等约束:bizType + bizId 唯一,重复调用不重复扣减
 * TODO: 资产服务(cex-asset-service)就绪后,以 Dubbo @DubboReference 实现本接口,冻结/解冻走 RPC
 */
public interface AccountServiceClient {

    void freeze(FreezeRequest request);

    void unfreeze(UnfreezeRequest request);
}
