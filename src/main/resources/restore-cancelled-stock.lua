-- 关单后的 Redis 库存回补。订单 ID 标记和加库存必须在同一脚本中完成，允许多次重试。
-- ARGV: voucherId, orderId
local markerKey = 'seckill:stock:restored:' .. ARGV[2]
if redis.call('exists', markerKey) == 1 then
    return 0
end
redis.call('incr', 'seckill:stock:' .. ARGV[1])
redis.call('set', markerKey, '1')
return 1
