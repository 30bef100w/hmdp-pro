package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final Long CACHE_VOUCHER_TTL = 30L;
    public static final String CACHE_VOUCHER_KEY = "cache:voucher:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /** 已执行的关单 Redis 库存回补标记，Lua 使用订单 ID 保证只加一次 */
    public static final String SECKILL_STOCK_RESTORED_KEY = "seckill:stock:restored:";
    /** 秒杀成功 claim 用户集合（Lua 脚本 SADD 写入，一人一单与补单差集依据） */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    /** RocketMQ 事务消息本地标记（与 Lua 扣库存同脚本写入，回查用） */
    public static final String SECKILL_TXN_KEY = "seckill:txn:";
    /** 事务标记 TTL（秒），需覆盖 Broker 回查窗口 */
    public static final long SECKILL_TXN_TTL_SECONDS = 3600L;
    /** 方案 B 排队/结果状态 seckill:queue:{orderId} */
    public static final String SECKILL_QUEUE_KEY = "seckill:queue:";
    /** 方案 B 排队订单所属用户 seckill:queue:user:{orderId} */
    public static final String SECKILL_QUEUE_USER_KEY = "seckill:queue:user:";
    /** 方案 B 重复下单时已有订单号 seckill:queue:existing-order:{orderId} */
    public static final String SECKILL_QUEUE_EXISTING_ORDER_KEY = "seckill:queue:existing-order:";
    /** 排队状态 TTL（分钟） */
    public static final long SECKILL_QUEUE_TTL_MINUTES = 5L;
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";

    /** 订单状态：1未支付 2已支付 3已核销 4已取消 5退款中 6已退款 */
    public static final int ORDER_STATUS_UNPAID = 1;
    public static final int ORDER_STATUS_PAID = 2;
    public static final int ORDER_STATUS_CANCELLED = 4;
}
