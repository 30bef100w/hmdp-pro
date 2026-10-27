package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RocketMQConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;

/**
 * 秒杀订单消费者：消费 CREATE（异步落库）与 TIMEOUT（超时关单）消息
 * 消费失败返回 RECONSUME_LATER，由 RocketMQ 按重试间隔自动重投，配合业务幂等保证不丢单
 */
@Slf4j
@Component
public class OrderMQConsumer {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.consumer.max-reconsume-times:16}")
    private int maxReconsumeTimes;

    private final IVoucherOrderService voucherOrderService;

    public OrderMQConsumer(IVoucherOrderService voucherOrderService) {
        this.voucherOrderService = voucherOrderService;
    }

    private DefaultMQPushConsumer consumer;

    @PostConstruct
    public void init() throws Exception {
        consumer = new DefaultMQPushConsumer(RocketMQConstants.ORDER_CONSUMER_GROUP);
        consumer.setNamesrvAddr(nameServer);
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);
        // 订单消息逐条确认，避免同一批中前面的成功消息因后续失败而重复消费。
        consumer.setConsumeMessageBatchMaxSize(1);
        // 消费者组首次启动时从最早位置消费；之后从上次消费位点续读
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(RocketMQConstants.ORDER_TOPIC, "CREATE || TIMEOUT");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                String tag = msg.getTags();
                String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                try {
                    if (RocketMQConstants.ORDER_TAG_CREATE.equals(tag)) {
                        VoucherOrder order = JSONUtil.toBean(body, VoucherOrder.class);
                        voucherOrderService.createOrderFromMQ(order);
                    } else if (RocketMQConstants.ORDER_TAG_TIMEOUT.equals(tag)) {
                        voucherOrderService.cancelTimeoutOrder(Long.valueOf(body));
                    } else {
                        log.warn("未知消息 Tag: {}", tag);
                    }
                } catch (Exception e) {
                    log.error("订单消息处理失败, tag={}, msgId={}, reconsumeTimes={}, body={}",
                            tag, msg.getMsgId(), msg.getReconsumeTimes(), body, e);
                    // 消费失败稍后重试；业务处理本身幂等，重复消费安全
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        log.info("RocketMQ 消费者启动成功，订阅 {}/CREATE || TIMEOUT", RocketMQConstants.ORDER_TOPIC);
    }

    @PreDestroy
    public void destroy() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
