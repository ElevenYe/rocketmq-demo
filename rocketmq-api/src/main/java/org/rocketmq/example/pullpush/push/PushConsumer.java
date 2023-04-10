package org.rocketmq.example.pullpush.push;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.rocketmq.example.util.ConfigUtil;

import java.util.List;

/**
 * 消费者被动接收生产者推送到 RocketMQ 的消息进行消费
 */
public class PushConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "PushProducerTestGroup";
    private static final String TOPIC = "Topic-PushProducerMessage";

    public static void main(String[] args) throws InterruptedException, MQClientException {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        // 设置NameServer地址
        consumer.setNamesrvAddr(NAMESRV_ADDRS);
        // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
        consumer.subscribe(TOPIC, "*");
        // 注册回调接口来处理从 Broker 中收到的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * msgList 是从 Broker 端获取的需要被消费消息列表，用户实现该接口，并把自己对消息的消费逻辑写在 consumeMessage 方法中，然后
             * 返回消费状态
             */
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgList);
                // 返回消息消费状态
                // ConsumeConcurrentlyStatus.CONSUME_SUCCESS 为消费成功
                // RECONSUME_LATER 表示消费失败，一段时间后再重新消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动Consumer
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
