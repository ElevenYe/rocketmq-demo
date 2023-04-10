package org.rocketmq.example.pullpush.litepullassign;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.rocketmq.example.util.ConfigUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * 可以指定从某一个 queue 中拉取消息，并且指定从哪个具体的消费点位开始进行消费。
 * 与Subscribe模式不同的是，Assign模式下没有自动的负载均衡机制，需要用户自行指定需要拉取的队列。
 */
public class AssignLitePullConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "AssignLitePullProducerTestGroup";
    private static final String TOPIC = "Topic-AssignLitePullProducerMessage";
    public static volatile boolean running = true;
    public static void main(String[] args) throws Exception {
        DefaultLitePullConsumer litePullConsumer = new DefaultLitePullConsumer(PRODUCER_GROUP);
        litePullConsumer.setNamesrvAddr(NAMESRV_ADDRS);
        // AutoCommit为false，表明关闭自动提交，后面使用 commitSync 方法手动提交
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();

        Collection<MessageQueue> mqSet = litePullConsumer.fetchMessageQueues(TOPIC);
        List<MessageQueue> list = new ArrayList<MessageQueue>(mqSet);
        List<MessageQueue> assignList = new ArrayList<MessageQueue>();
        // 取消息队列的前 4 个队列中的消息进行消费（服务器上共有 8 个队列，两台服务，每台服务有 4 个队列）
        for (int i = 0; i < list.size() / 2; i++) {
            assignList.add(list.get(i));
        }
        litePullConsumer.assign(assignList);
        // 从第一个队列的第 0 个消费点位开始消费
        litePullConsumer.seek(assignList.get(0), 0);
        try {
            while (running) {
                // 循环不停地调用poll方法拉取消息
                List<MessageExt> messageExtList = litePullConsumer.poll();
                System.out.printf("%s %n", messageExtList);
                // 拉取到消息后调用 commitSync 方法手动提交位点
                litePullConsumer.commitSync();
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }
}
