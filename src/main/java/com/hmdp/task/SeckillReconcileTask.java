package com.hmdp.task;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mq.RocketMQProducer;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SeckillMode;

import static com.hmdp.utils.RedisConstants.SECKILL_STOCK_KEY;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 秒杀对账任务：兜底 MQ 消息丢失与库存漂移
 * <p>
 * 核心思想：Redis/DB 库存都不是真相，订单表才是唯一账本，库存是派生值。
 * 每轮按 ①关单兜底 → ②补单 → ③库存重算 顺序执行，每轮收敛。
 * 订单表修复（①②）必须在库存重算（③）之前：账本先修对，重算才准确。
 */
@Slf4j
@Component
public class SeckillReconcileTask {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private RocketMQProducer rocketMQProducer;
    @Resource
    private RedisIdWorker redisIdWorker;

    /** 超时关单阈值：与延迟消息档位一致（15 分钟） */
    private static final int ORDER_TIMEOUT_MINUTES = 15;
    /** 秒杀结束多久后允许重算库存（等待消费队列排空） */
    private static final int RECONCILE_AFTER_END_MINUTES = 2;

    /**
     * 每分钟执行：关单兜底 → 补单 → 库存重算。
     * 多实例部署时用分布式锁保证同一时刻只有一个实例在对账；任务本身幂等，重复执行无副作用。
     */
    @Scheduled(fixedDelay = 60_000)
    public void reconcile() {
        RLock lock = redissonClient.getLock("lock:seckill:reconcile");
        if (!lock.tryLock()) {
            return; // 其他实例正在对账
        }
        try {
            repairMissingRedisStocks();
            closeTimeoutOrders();
            restoreCancelledOrderRedisStocks();
            supplementMissingOrders();
            reconcileFinishedStocks();
        } catch (Exception e) {
            log.error("秒杀对账任务执行异常", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 发布事务提交后 Redis 初始化失败时补建库存键。
     * 只有尚无 claim 用户的活动券才允许按 DB 库存初始化，避免覆盖正在进行的秒杀库存。
     */
    private void repairMissingRedisStocks() {
        List<SeckillVoucher> active = seckillVoucherService.lambdaQuery()
                .ge(SeckillVoucher::getEndTime, LocalDateTime.now())
                .list();
        for (SeckillVoucher voucher : active) {
            Long voucherId = voucher.getVoucherId();
            String stockKey = SECKILL_STOCK_KEY + voucherId;
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(stockKey))) {
                continue;
            }
            Long claimed = stringRedisTemplate.opsForSet()
                    .size(RedisConstants.SECKILL_ORDER_KEY + voucherId);
            if (claimed != null && claimed > 0) {
                log.error("Redis 库存缺失但已有 claim 用户，跳过自动重置，voucherId={}, claimed={}",
                        voucherId, claimed);
                continue;
            }
            if (voucher.getStock() == null) {
                log.error("Redis 库存缺失且 DB 库存为空，跳过补偿，voucherId={}", voucherId);
                continue;
            }
            try {
                stringRedisTemplate.opsForValue().setIfAbsent(stockKey, String.valueOf(voucher.getStock()));
                log.warn("补偿初始化秒杀券 Redis 库存，voucherId={}, stock={}", voucherId, voucher.getStock());
            } catch (Exception e) {
                log.error("补偿初始化秒杀券 Redis 库存失败，voucherId={}", voucherId, e);
            }
        }
    }

    /**
     * ① 关单兜底：兜 TIMEOUT 延迟消息丢失。
     * 复用 cancelTimeoutOrder（CAS: WHERE status=未支付），与延迟消息并发执行也不重复回补库存。
     */
    private void closeTimeoutOrders() {
        List<VoucherOrder> expired = voucherOrderService.lambdaQuery()
                .eq(VoucherOrder::getStatus, RedisConstants.ORDER_STATUS_UNPAID)
                .lt(VoucherOrder::getCreateTime, LocalDateTime.now().minusMinutes(ORDER_TIMEOUT_MINUTES))
                .list();
        for (VoucherOrder order : expired) {
            voucherOrderService.cancelTimeoutOrder(order.getId());
        }
        if (!expired.isEmpty()) {
            // expired.size() 是扫描数，CAS 会跳过已支付/已取消的，实际关闭数 ≤ 扫描数
            log.warn("对账关单兜底：扫描到 {} 笔超时未支付订单（CAS 幂等，已处理过的自动跳过）", expired.size());
        }
    }

    /** 关单 DB 事务已提交但 Redis 回补失败时，按订单 ID 幂等重试。 */
    private void restoreCancelledOrderRedisStocks() {
        List<VoucherOrder> pending = voucherOrderService.lambdaQuery()
                .eq(VoucherOrder::getStatus, RedisConstants.ORDER_STATUS_CANCELLED)
                .eq(VoucherOrder::getRedisStockRestored, false)
                .last("LIMIT 200")
                .list();
        for (VoucherOrder order : pending) {
            try {
                voucherOrderService.restoreCancelledOrderRedisStock(order.getId());
            } catch (Exception e) {
                log.error("订单 {} Redis 库存回补重试失败", order.getId(), e);
            }
        }
    }

    /**
     * ② 补单：兜 CREATE 消息丢失（Redis 已扣库存、订单表缺失）。
     * 只对已结束的券执行——进行中的秒杀，claim 后消费者存在秒级落库延迟，
     * 对账过早跑差集会误判丢单（白打 MQ、日志误报）。
     * seckill:order:{voucherId}（Lua claim 用户）与订单表已落库用户的差集 = 丢单；
     * 重新发号补发 CREATE 消息，消费者幂等链（锁→count→唯一索引）保证不重复建单。
     */
    private void supplementMissingOrders() {
        List<SeckillVoucher> vouchers = seckillVoucherService.lambdaQuery()
                .lt(SeckillVoucher::getEndTime, LocalDateTime.now())
                .list();
        for (SeckillVoucher voucher : vouchers) {
            Long voucherId = voucher.getVoucherId();
            Set<String> claimed = stringRedisTemplate.opsForSet()
                    .members(RedisConstants.SECKILL_ORDER_KEY + voucherId);
            if (claimed == null || claimed.isEmpty()) {
                continue;
            }
            List<VoucherOrder> created = voucherOrderService.lambdaQuery()
                    .select(VoucherOrder::getUserId)
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .list();
            Set<String> createdSet = created.stream()
                    .map(o -> String.valueOf(o.getUserId()))
                    .collect(Collectors.toSet());

            for (String userId : claimed) {
                if (createdSet.contains(userId)) {
                    continue;
                }
                VoucherOrder order = new VoucherOrder();
                order.setId(redisIdWorker.nextId("order"));
                order.setUserId(Long.valueOf(userId));
                order.setVoucherId(voucherId);
                // 补单场景：入口已扣 Redis（claim 已在集合中），消费者走方案 A 路径直接落库，不再 claim
                order.setSeckillMode(SeckillMode.A);
                try {
                    rocketMQProducer.sendOrderCreate(order);
                    log.warn("对账补单：voucherId={}, userId={}, 新订单号={}", voucherId, userId, order.getId());
                } catch (Exception e) {
                    // 发送失败不抛异常，下轮对账再次尝试；消费者幂等，重复补发安全
                    log.error("对账补单发送失败，voucherId={}, userId={}", voucherId, userId, e);
                }
            }
        }
    }

    /**
     * ③ 库存重算：只对已结束（且结束超过 2 分钟，队列基本排空）的券。
     * expected = initial_stock − 有效订单数(未支付+已支付)，Redis 与 DB 一起改写为 expected。
     * 预期值只来自订单表账本，不存在"修错方向"；每轮收敛，重复执行安全。
     * Redis 与 DB 均已一致则跳过写库。
     */
    private void reconcileFinishedStocks() {
        List<SeckillVoucher> finished = seckillVoucherService.lambdaQuery()
                .lt(SeckillVoucher::getEndTime, LocalDateTime.now().minusMinutes(RECONCILE_AFTER_END_MINUTES))
                .list();
        for (SeckillVoucher voucher : finished) {
            Long voucherId = voucher.getVoucherId();
            int initial = voucher.getInitialStock() == null ? 0 : voucher.getInitialStock();
            if (initial <= 0) {
                // 无账本基准（存量数据未回填/发布时未赋值），跳过重算避免误刷库存
                log.warn("对账库存重算跳过：voucherId={} 缺少 initialStock，请确认是否执行回填 SQL", voucherId);
                continue;
            }
            long valid = voucherOrderService.lambdaQuery()
                    .eq(VoucherOrder::getVoucherId, voucherId)
                    .in(VoucherOrder::getStatus,
                            RedisConstants.ORDER_STATUS_UNPAID, RedisConstants.ORDER_STATUS_PAID)
                    .count();
            int expected = Math.max(0, initial - (int) valid);

            boolean dbOk = voucher.getStock() != null && voucher.getStock() == expected;
            String redisStock = stringRedisTemplate.opsForValue().get(SECKILL_STOCK_KEY + voucherId);
            boolean redisOk = String.valueOf(expected).equals(redisStock);
            if (dbOk && redisOk) {
                continue; // 两边已一致，跳过写库
            }
            log.warn("对账库存重算：voucherId={}, db={}, redis={} → expected={}（初始库存={}, 有效订单={}）",
                    voucherId, voucher.getStock(), redisStock, expected, initial, valid);
            // 告警与修复同一步：差异已记录，Redis 与 DB 统一改写为 expected
            stringRedisTemplate.opsForValue().set(
                    RedisConstants.SECKILL_STOCK_KEY + voucherId, String.valueOf(expected));
            seckillVoucherService.update(
                    Wrappers.<SeckillVoucher>lambdaUpdate()
                            .set(SeckillVoucher::getStock, expected)
                            .eq(SeckillVoucher::getVoucherId, voucherId));
        }
    }
}
