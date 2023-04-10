package org.rocketmq.example.filtermessage.tagfilter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.rocketmq.example.util.ConfigUtil;

import java.util.List;

/**
 * Tag消息过滤-消息消费者
 * 消息过滤是指消息生产者向Topic中发送消息时，设置消息属性对消息进行分类，消费者订阅Topic时，根据消息属性设置过滤条件对消息进行过滤，只有符合过
 * 滤条件的消息才会被投递到消费端进行消费。
 *
 * 消费者订阅Topic时若未设置过滤条件，无论消息发送时是否有设置过滤属性，Topic中的所有消息都将被投递到消费端进行消费。
 */
public class TagFilterConsumer {
    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerTagFilterTestGroup";
    private static final String TOPIC = "Topic-ProducerTagFilterMessage";

    public static void main(String[] args) throws MQClientException {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        // 设置NameServer地址
        consumer.setNamesrvAddr(NAMESRV_ADDRS);
        // 订阅一个或多个topic，并指定tag过滤条件
        consumer.subscribe(TOPIC, "TagA || TagB");
        // 注册回调接口来处理从Broker中收到的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 返回消息消费状态，ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动Consumer
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
