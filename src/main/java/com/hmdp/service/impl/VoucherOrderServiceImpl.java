package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.RocketMQProducer;
import com.hmdp.mq.SeckillTxContext;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SeckillMode;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RocketMQProducer rocketMQProducer;
    @Resource
    private PlatformTransactionManager transactionManager;

    /** 秒杀方案：A=入口预扣+事务消息；B=限流入队+消费者校验 */
    @Value("${seckill.mode:A}")
    private String seckillMode;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    /** 对比测试：只扣库存、不做 Lua 一人一单 */
    private static final DefaultRedisScript<Long> SECKILL_STOCK_ONLY_SCRIPT;
    /** 方案 B 消费者：Redis claim 库存+一人一单 */
    private static final DefaultRedisScript<Long> SECKILL_CLAIM_SCRIPT;
    /** 关单后按订单 ID 幂等回补 Redis 库存 */
    private static final DefaultRedisScript<Long> RESTORE_CANCELLED_STOCK_SCRIPT;

    /** Redis 开关：seckill:test:protection = FULL|HEIMA|EARLY（默认 FULL） */
    public static final String ONE_ORDER_PROTECTION_KEY = "seckill:test:protection";

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        SECKILL_STOCK_ONLY_SCRIPT = new DefaultRedisScript<>();
        SECKILL_STOCK_ONLY_SCRIPT.setLocation(new ClassPathResource("seckill-stock-only.lua"));
        SECKILL_STOCK_ONLY_SCRIPT.setResultType(Long.class);

        SECKILL_CLAIM_SCRIPT = new DefaultRedisScript<>();
        SECKILL_CLAIM_SCRIPT.setLocation(new ClassPathResource("seckill-claim.lua"));
        SECKILL_CLAIM_SCRIPT.setResultType(Long.class);

        RESTORE_CANCELLED_STOCK_SCRIPT = new DefaultRedisScript<>();
        RESTORE_CANCELLED_STOCK_SCRIPT.setLocation(new ClassPathResource("restore-cancelled-stock.lua"));
        RESTORE_CANCELLED_STOCK_SCRIPT.setResultType(Long.class);
    }

    private String oneOrderProtection() {
        String mode = stringRedisTemplate.opsForValue().get(ONE_ORDER_PROTECTION_KEY);
        return mode == null || mode.isEmpty() ? "FULL" : mode.trim().toUpperCase();
    }

    /**
     * 取消超时订单：仅当订单仍处于未支付状态时回补库存
     * 由 RocketMQ 延迟消息触发；通过状态乐观更新保证幂等
     */
    @Transactional(rollbackFor = Exception.class)
    public void cancelTimeoutOrder(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null || order.getStatus() == null || order.getStatus() != ORDER_STATUS_UNPAID) {
            return;
        }

        // 状态置为已取消
        boolean updated = update()
                .set("status", ORDER_STATUS_CANCELLED)
                .set("redis_stock_restored", false)
                .eq("id", orderId)
                .eq("status", ORDER_STATUS_UNPAID)
                .update();
        if (!updated) {
            // 并发下已支付，无需处理
            return;
        }

        // 先在同一个数据库事务中回补 DB 库存。数据库失败时订单状态也会回滚，MQ 可以安全重试。
        boolean stockUpdated = seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", order.getVoucherId())
                .update();
        if (!stockUpdated) {
            throw new IllegalStateException("秒杀券不存在，无法回补库存: " + order.getVoucherId());
        }

        // 只在 DB 提交后补 Redis；进程中断或 Redis 故障时，由待补标记驱动定时重试。
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            @Override
            public void afterCommit() {
                try {
                    restoreCancelledOrderRedisStock(orderId);
                } catch (Exception e) {
                    log.error("订单 {} 已取消，但 Redis 库存回补失败，等待定时重试，券 {}",
                            orderId, order.getVoucherId(), e);
                }
            }
        });
        log.info("订单 {} 超时未支付，已取消并回补 DB 库存，券 {}", orderId, order.getVoucherId());
    }

    @Override
    public void restoreCancelledOrderRedisStock(Long orderId) {
        VoucherOrder order = getById(orderId);
        if (order == null || order.getStatus() != ORDER_STATUS_CANCELLED
                || !Boolean.FALSE.equals(order.getRedisStockRestored())) {
            return;
        }
        Long restored = stringRedisTemplate.execute(
                RESTORE_CANCELLED_STOCK_SCRIPT,
                Collections.emptyList(),
                order.getVoucherId().toString(), orderId.toString()
        );
        if (restored == null) {
            throw new IllegalStateException("Redis 库存回补脚本无结果, orderId=" + orderId);
        }
        // afterCommit 回调仍持有原事务资源；用新事务持久化完成标记。
        TransactionTemplate requiresNew = new TransactionTemplate(transactionManager);
        requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        Boolean marked = requiresNew.execute(status -> update()
                .set("redis_stock_restored", true)
                .eq("id", orderId)
                .eq("status", ORDER_STATUS_CANCELLED)
                .eq("redis_stock_restored", false)
                .update());
        if (marked) {
            log.info("订单 {} Redis 库存回补完成，券 {}", orderId, order.getVoucherId());
        }
    }

    @Override
    public Result payOrder(Long orderId) {
        Long currentUserId = currentUserId();
        if (currentUserId == null) {
            return Result.fail("请先登录");
        }
        VoucherOrder order = getById(orderId);
        if (order == null || !currentUserId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }

        // 模拟支付：仅当前用户且订单仍未支付时改为已支付
        boolean updated = update()
                .set("status", ORDER_STATUS_PAID)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("user_id", currentUserId)
                .eq("status", ORDER_STATUS_UNPAID)
                .update();
        if (!updated) {
            return Result.fail("订单不存在或状态不允许支付");
        }
        // 延迟关单消息不可撤销，到点后 cancelTimeoutOrder 通过状态乐观更新自动跳过已支付订单，无需额外处理
        return Result.ok("支付成功");
    }

    /**
     * MQ 消费者异步落库入口：幂等创建订单
     * 方案 A：入口已扣 Redis，此处落库 + DB 二次扣库存
     * 方案 B：此处先 claim Redis，再落库，并回写排队终态
     */
    @Transactional(rollbackFor = Exception.class)
    public void createOrderFromMQ(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        Long orderId = voucherOrder.getId();
        if (userId == null || voucherId == null || orderId == null) {
            log.error("订单消息数据不完整，丢弃: {}", voucherOrder);
            return;
        }
        boolean modeB = SeckillMode.B.equalsIgnoreCase(voucherOrder.getSeckillMode());
        String protection = oneOrderProtection();
        // EARLY=黑马最初仅靠事后 count（本分支直接不加锁、不预查，制造并发窗口）
        // HEIMA=Lua + Redisson + count（课程终态常见组合，不含唯一索引兜底语义）
        // FULL=三级：Lua + Redisson + count + 唯一索引捕获
        boolean early = "EARLY".equals(protection);
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        boolean locked = false;
        if (!early) {
            locked = redisLock.tryLock();
            if (!locked) {
                if (modeB) {
                    throw new RuntimeException("获取下单锁失败，等待重试, orderId=" + orderId);
                }
                log.error("不允许重复下单！");
                return;
            }
        }

        try {
            if (!early) {
                int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
                if (count > 0) {
                    if (modeB) {
                        writeQueueStatus(orderId, SeckillMode.QUEUE_SUCCESS);
                    } else {
                        log.error("不允许重复下单！");
                    }
                    return;
                }
            }

            if (modeB) {
                Long claim = stringRedisTemplate.execute(
                        SECKILL_CLAIM_SCRIPT,
                        Collections.emptyList(),
                        voucherId.toString(), userId.toString()
                );
                if (claim == null || claim == 1L) {
                    writeQueueStatus(orderId, SeckillMode.QUEUE_FAIL_STOCK);
                    log.warn("方案B库存不足, orderId={}, userId={}, voucherId={}", orderId, userId, voucherId);
                    return;
                }
            }

            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0)
                    .update();
            if (!success) {
                if (modeB) {
                    stringRedisTemplate.opsForValue().increment(SECKILL_STOCK_KEY + voucherId);
                    stringRedisTemplate.opsForSet().remove("seckill:order:" + voucherId, userId.toString());
                    writeQueueStatus(orderId, SeckillMode.QUEUE_FAIL_STOCK);
                }
                log.error("库存不足！orderId={}", orderId);
                return;
            }

            voucherOrder.setStatus(ORDER_STATUS_UNPAID);
            try {
                save(voucherOrder);
            } catch (DuplicateKeyException e) {
                boolean restored = seckillVoucherService.update()
                        .setSql("stock = stock + 1")
                        .eq("voucher_id", voucherId)
                        .update();
                if (!restored) {
                    throw new IllegalStateException("重复订单回补库存失败, voucherId=" + voucherId, e);
                }
                if (modeB) {
                    writeQueueStatus(orderId, SeckillMode.QUEUE_SUCCESS);
                }
                log.warn("重复订单消息，唯一索引拦截并回补DB库存, userId={}, voucherId={}", userId, voucherId);
                return;
            }
            try {
                rocketMQProducer.sendOrderTimeout(voucherOrder.getId());
            } catch (Exception e) {
                log.error("超时关单延迟消息发送失败，订单 {} 缺少自动关单保障", voucherOrder.getId(), e);
            }
            if (modeB) {
                writeQueueStatus(orderId, SeckillMode.QUEUE_SUCCESS);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            log.error("MQ 异步落库失败，等待重试, orderId={}", orderId, e);
            throw new RuntimeException(e);
        } finally {
            if (locked) {
                redisLock.unlock();
            }
        }
    }

    /**
     * 事务消息本地事务：Lua 库存 + 一人一单 + 写 seckill:txn 标记
     */
    @Override
    public long executeSeckillLocalTransaction(Long voucherId, Long userId, Long orderId) {
        String protection = oneOrderProtection();
        DefaultRedisScript<Long> script = "EARLY".equals(protection) ? SECKILL_STOCK_ONLY_SCRIPT : SECKILL_SCRIPT;
        Long result = stringRedisTemplate.execute(
                script,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        return result == null ? 1L : result;
    }

    @Override
    public boolean hasSeckillTxnMarker(Long orderId) {
        Boolean exists = stringRedisTemplate.hasKey(SECKILL_TXN_KEY + orderId);
        return Boolean.TRUE.equals(exists);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        if (SeckillMode.B.equalsIgnoreCase(seckillMode)) {
            return seckillVoucherModeB(voucherId);
        }
        return seckillVoucherModeA(voucherId);
    }

    /**
     * 方案 A：事务消息 + 入口 Lua（库存/一人一单），同步返回成败
     */
    private Result seckillVoucherModeA(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        order.setSeckillMode(SeckillMode.A);

        SeckillTxContext ctx = new SeckillTxContext(order);
        try {
            SendResult sendResult = rocketMQProducer.sendOrderCreateInTransaction(order, ctx);
            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                log.error("事务半消息发送失败, orderId={}, status={}", orderId, sendResult.getSendStatus());
                return Result.fail("系统繁忙，请稍后重试");
            }
        } catch (Exception e) {
            log.error("事务消息发送异常, userId={}, voucherId={}", userId, voucherId, e);
            return Result.fail("系统繁忙，请稍后重试");
        }

        long r = ctx.getLuaResult();
        if (r == 0) {
            return Result.ok(orderId);
        }
        if (r == 1) {
            return Result.fail("库存不足");
        }
        if (r == 2) {
            return Result.fail("不能重复下单");
        }
        return Result.fail("系统繁忙，请稍后重试");
    }

    /**
     * 方案 B：入口只发消息 + 写 WAITING，立即返回 orderId；限流在校验前由网关挡，校验在消费者
     */
    private Result seckillVoucherModeB(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder order = new VoucherOrder();
        order.setId(orderId);
        order.setUserId(userId);
        order.setVoucherId(voucherId);
        order.setSeckillMode(SeckillMode.B);

        writeQueueStatus(orderId, SeckillMode.QUEUE_WAITING);
        writeQueueOwner(orderId, userId);
        try {
            SendResult sendResult = rocketMQProducer.sendOrderCreate(order);
            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                writeQueueStatus(orderId, SeckillMode.QUEUE_FAIL_SYSTEM);
                return Result.fail("系统繁忙，请稍后重试");
            }
        } catch (Exception e) {
            log.error("方案B发消息失败, userId={}, voucherId={}", userId, voucherId, e);
            writeQueueStatus(orderId, SeckillMode.QUEUE_FAIL_SYSTEM);
            return Result.fail("系统繁忙，请稍后重试");
        }
        // 返回 orderId：前端应轮询 /voucher-order/seckill/result/{orderId}
        return Result.ok(orderId);
    }

    @Override
    public Result getSeckillResult(Long orderId) {
        if (orderId == null) {
            return Result.fail("订单号不能为空");
        }
        Map<String, Object> data = new HashMap<>(4);
        data.put("orderId", orderId);
        data.put("mode", seckillMode);

        Long currentUserId = currentUserId();
        if (currentUserId == null) {
            return Result.fail("请先登录");
        }
        String status = stringRedisTemplate.opsForValue().get(SECKILL_QUEUE_KEY + orderId);
        String owner = stringRedisTemplate.opsForValue().get(SECKILL_QUEUE_USER_KEY + orderId);
        if (owner != null && !owner.equals(currentUserId.toString())) {
            return Result.fail("订单不存在");
        }

        // 排队状态和所属用户键同时过期时，以下面的订单表归属作为最终校验。
        VoucherOrder order = getById(orderId);
        if (order != null && !currentUserId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (status != null) {
            if (owner == null && order == null) {
                return Result.fail("订单不存在");
            }
            data.put("status", status);
            if (order != null) {
                data.put("orderStatus", order.getStatus());
            }
            return Result.ok(data);
        }
        // 无排队状态：方案 A 或状态已过期 → 查订单表兜底
        if (order != null) {
            data.put("status", SeckillMode.QUEUE_SUCCESS);
            data.put("orderStatus", order.getStatus());
            return Result.ok(data);
        }
        data.put("status", "NOT_FOUND");
        return Result.ok(data);
    }

    private void writeQueueStatus(Long orderId, String status) {
        stringRedisTemplate.opsForValue().set(
                SECKILL_QUEUE_KEY + orderId,
                status,
                SECKILL_QUEUE_TTL_MINUTES,
                TimeUnit.MINUTES
        );
        // 失败结果没有订单表记录，所属用户标记必须与结果状态一起保留。
        stringRedisTemplate.expire(
                SECKILL_QUEUE_USER_KEY + orderId,
                SECKILL_QUEUE_TTL_MINUTES,
                TimeUnit.MINUTES
        );
    }

    private void writeQueueOwner(Long orderId, Long userId) {
        stringRedisTemplate.opsForValue().set(
                SECKILL_QUEUE_USER_KEY + orderId,
                userId.toString(),
                SECKILL_QUEUE_TTL_MINUTES,
                TimeUnit.MINUTES
        );
    }

    private Long currentUserId() {
        return UserHolder.getUser() == null ? null : UserHolder.getUser().getId();
    }

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);

        // 3.返回订单id
        return Result.ok(orderId);
    }*/
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        return createVoucherOrder(voucherId);
    }



    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/
    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock(1200);
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/

    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        }
    }*/
}
