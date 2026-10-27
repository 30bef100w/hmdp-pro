package com.hmdp.mq;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.utils.MultiLevelCacheService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * Canal binlog 缓存驱逐：MySQL 变更 → RocketMQ → 清除 Caffeine + Redis
 * 监听 tb_shop，配合 OpenResty L1 / Java 多级缓存做最终一致。
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "canal", name = "enabled", havingValue = "true")
public class CanalMQConsumer {

    public static final String CANAL_TOPIC = "canal-binlog-topic";

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${canal.consumer.max-reconsume-times:16}")
    private int maxReconsumeTimes;

    @Resource
    private MultiLevelCacheService multiLevelCacheService;

    private DefaultMQPushConsumer consumer;

    private final Map<String, Consumer<JSONObject>> tableHandlers = new HashMap<>();

    @PostConstruct
    public void init() throws Exception {
        tableHandlers.put("tb_shop", this::evictShop);

        consumer = new DefaultMQPushConsumer("canal-cache-consumer-group");
        consumer.setNamesrvAddr(nameServer);
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);
        // 单条消费，避免一个坏事件导致同批已成功事件重复投递。
        consumer.setConsumeMessageBatchMaxSize(1);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(CANAL_TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    handleBinlogEvent(new String(msg.getBody(), StandardCharsets.UTF_8));
                } catch (Exception e) {
                    log.error("binlog 事件处理失败, msgId={}, reconsumeTimes={}",
                            msg.getMsgId(), msg.getReconsumeTimes(), e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        log.info("Canal 缓存驱逐消费者启动成功，监听 topic={}", CANAL_TOPIC);
    }

    private void handleBinlogEvent(String body) {
        JSONObject flat = JSONUtil.parseObj(body);
        String table = flat.getStr("table");
        Consumer<JSONObject> handler = tableHandlers.get(table);
        if (handler == null) {
            return;
        }
        JSONArray data = flat.getJSONArray("data");
        if (data == null || data.isEmpty()) {
            return;
        }
        for (Object row : data) {
            handler.accept((JSONObject) row);
        }
    }

    private void evictShop(JSONObject row) {
        String id = row.getStr("id");
        if (id == null) {
            return;
        }
        multiLevelCacheService.evict(CACHE_SHOP_KEY, id);
        log.info("[Canal] tb_shop 变更，已驱逐缓存 cache:shop:{}", id);
    }

    @PreDestroy
    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
