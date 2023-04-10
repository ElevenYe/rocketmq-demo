package org.rocketmq.example.pullpush.litepullsubscribe;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.rocketmq.example.util.ConfigUtil;

import java.util.List;

/**
 * 与Push Consumer不同的是，LitePullConsumer拉取消息调用的是轮询poll接口，如果能拉取到消息则返回对应的消息列表，否则返回null。
 * 在subscribe模式下，同一个消费组下的多个LitePullConsumer会负载均衡消费
 */
public class SubscribeLitePullConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "SubscribeLitePullProducerTestGroup";
    private static final String TOPIC = "Topic-SubscribeLitePullProducerMessage";
    public static volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(PRODUCER_GROUP);
        litePullConsumer.setNamesrvAddr(NAMESRV_ADDRS);
        // 订阅一个或多个topic，并指定 tag 过滤条件，这里指定*表示接收所有tag的消息
        litePullConsumer.subscribe(TOPIC, "*");
        // litePullConsumer.setPollTimeoutMillis(10000);
        // litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 设置每一次拉取的最大消息数量，此外如果不额外设置，LitePullConsumer默认是自动提交位点。
        // litePullConsumer.setPullBatchSize(20);
        litePullConsumer.start();
        try {
            while (running) {
                List<MessageExt> messageExtList = litePullConsumer.poll();
                System.out.printf("%s%n", messageExtList);
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
