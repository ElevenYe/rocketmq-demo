package org.rocketmq.example.batchmessage;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.rocketmq.example.util.ConfigUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 批量消息发送-消息提供者
 */
public class BatchProducer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerBatchTestGroup";
    private static final String TOPIC = "Topic-ProducerBatchMessage";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDRS);
        producer.start();

        // If you just send messages of no more than 1MiB at a time, it is easy to use batch
        // Messages of the same batch should have: same topic, same waitStoreMsgOK and no schedule support
        List<Message> messages = new ArrayList<Message>();
        messages.add(new Message(TOPIC, "TagA", "OrderID001", "Hello RocketMQ 0".getBytes()));
        messages.add(new Message(TOPIC, "TagA", "OrderID002", "Hello RocketMQ 1".getBytes()));
        messages.add(new Message(TOPIC, "TagA", "OrderID003", "Hello RocketMQ 2".getBytes()));

        producer.send(messages);
    }
}
