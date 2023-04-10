package org.rocketmq.example.reconsumer.unordermsg;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.rocketmq.example.util.ConfigUtil;

import java.util.List;

/**
 * 无序消费重试（普通消息、延时消息、事务消息）
 * 当 Consumer 消息消费失败时，可以通过设置返回状态达到消息重试的效果。不过需要注意，无序消息的重试只对集群消费方式生效，广播消费方式不提供失败
 * 重试特性。即对于广播消费，消费失败后，失败的消息将不再重试，继续消费后续消息。
 *
 * 对于无序消息集群消费下的重试消费，每条消息默认最多重试16次，但每次重试的时间间隔是不同的，会逐渐变长
 *
 * 重试次数    与上次重试时间间隔
 *    1             10s
 *    2             30s
 *    3             1m
 *    4             2m
 *    5             3m
 *    6             4m
 *    7             5m
 *    8             6m
 *    9             7m
 *    10            8m
 *    11            9m
 *    12            10m
 *    13            20m
 *    14            30m
 *    15            1h
 *    16            2h
 *
 * 根据上述表可知，若一条消息一直重试失败，将会在正常消费后的第4小时46分进行第 16 次重试
 *
 * 我们可以通过修改 setMaxReconsumeTimes 的值来修改消费重试次数。
 * 如果修改值小于16次则重试间隔按照上述列表进行，如果大于16次，超出的次数的重试时间间隔均为 2h。
 * 重试次数的修改是作用在同一个消费者组的，若出现多个 Consumer 做来修改，则采用覆盖模式，取最近一次使用的重试次数
 */
public class ReConsumerUnorderMsgConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ReConsumerUnorderMsgProducerTestGroup";
    private static final String TOPIC = "Topic-ReConsumerUnorderMsgProducerMessage";

    public static void main(String[] args) throws MQClientException {
        // 初始化consumer，并设置consumer group name
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(PRODUCER_GROUP);
        // 设置NameServer地址
        consumer.setNamesrvAddr(NAMESRV_ADDRS);
        // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
        consumer.subscribe(TOPIC, "*");
        // 注册回调接口来处理从Broker中收到的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 触发消息重试的三种方式
                // 方式一：返回 ConsumeConcurrentlyStatus.RECONSUME_LATER （官方推荐方式）
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;

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
