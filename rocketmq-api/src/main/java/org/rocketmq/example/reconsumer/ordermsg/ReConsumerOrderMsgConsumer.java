package org.rocketmq.example.reconsumer.ordermsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.rocketmq.example.util.ConfigUtil;

import java.util.List;

/**
 * 顺序消费重试
 * 对于顺序消费，Consumer 消费失败后，为了保证消费的顺序性，会自动无间断地进行消息重试，直到消费成功。重试期间应用会出现消息消费被阻塞的情况。
 * 由于顺序消费重试是无休止的，无间断地进行消息重试，直到消费成功，所以务必要保证应用能够及时监控并处理消费失败的情况，避免消费被永久性阻塞。
 *
 * 顺序消费没有生产者发送失败重试，但是有消费者消费失败重试
 */
public class ReConsumerOrderMsgConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ReConsumerOrderMsgProducerTestGroup";
    private static final String TOPIC = "Topic-ReConsumerOrderMsgProducerMessage";

    public static void main(String[] args) throws MQClientException {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        // 设置NameServer地址
        consumer.setNamesrvAddr(NAMESRV_ADDRS);
        // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
        consumer.subscribe(TOPIC, "*");
        // 顺序消息消费失败的消费重试时间间隔，单位毫秒，默认为1000，其取值范围为[10, 30000]毫秒
        consumer.setSuspendCurrentQueueTimeMillis(100);
        // 注册顺序消费的回调接口来处理从 Broker 中收到的消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                context.setAutoCommit(false);
                // 触发消息重试的三种方式
                // 方式一：返回 ConsumeConcurrentlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT （官方推荐方式）
                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;

                // 方式二：返回 null
                // return null;

                // 方式三：抛出异常
                // throw new RuntimeException("消费重试测试！");
            }
        });
        // 启动Consumer
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
