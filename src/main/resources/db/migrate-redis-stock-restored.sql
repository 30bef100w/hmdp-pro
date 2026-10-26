-- 现有数据库升级时执行一次；新建数据库直接使用 hmdp.sql。
-- 已取消的历史订单默认视为回补完成，避免重复加 Redis 库存。
ALTER TABLE `tb_voucher_order`
    ADD COLUMN `redis_stock_restored` tinyint(1) NOT NULL DEFAULT 1
        COMMENT '关单后 Redis 库存是否已回补，0：待补，1：已补',
    ADD INDEX `idx_status_redis_stock_restored` (`status`, `redis_stock_restored`);
