package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    Result payOrder(Long orderId);

    /** 取消超时未支付订单（RocketMQ 延迟消息触发，回补库存） */
    void cancelTimeoutOrder(Long orderId);

    /** 重试关单后尚未完成的 Redis 库存回补 */
    void restoreCancelledOrderRedisStock(Long orderId);

    /** MQ 异步落库创建订单（幂等） */
    void createOrderFromMQ(VoucherOrder voucherOrder);

    /**
     * 事务消息本地事务：执行 Lua（库存+一人一单+事务标记）
     * @return 0 成功；1 库存不足；2 重复下单
     */
    long executeSeckillLocalTransaction(Long voucherId, Long userId, Long orderId);

    /** 事务回查：判断 seckill:txn:{orderId} 是否存在 */
    boolean hasSeckillTxnMarker(Long orderId);

    /**
     * 查询秒杀异步结果（方案 B 为主；方案 A 成功时无排队状态，可查订单表兜底）
     * @return data 含 orderId、status（WAITING/SUCCESS/FAIL_*）
     */
    Result getSeckillResult(Long orderId);
}
