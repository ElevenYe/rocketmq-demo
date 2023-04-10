package org.rocketmq.example.reconsumer.ordermsg;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.rocketmq.example.util.ConfigUtil;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 顺序消费重试的消息生产者
 */
public class ReConsumerOrderMsgProducer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ReConsumerOrderMsgProducerTestGroup";
    private static final String TOPIC = "Topic-ReConsumerOrderMsgProducerMessage";

    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException,
            MQClientException, UnsupportedEncodingException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP); //（1）
        // 设置NameServer地址
        producer.setNamesrvAddr(NAMESRV_ADDRS);  //（2）
        // 设置超时时间，消息默认发送的超时时间为3秒，一旦响应时间超过该超时时间将会报错
        producer.setSendMsgTimeout(10000);
        producer.start();

        // RocketMQ集群服务器上有2台服务，8个队列
        // 下述代码将分别把 5（j） 条消息放入到 8（i） 个不同的队列中，orderId将会对应不同的queue
        for (int i = 0; i < 8; i++) {
            int orderId = i;

            for (int j = 0; j < 5; j++) {
                Message msg = new Message(TOPIC /* topic */,
                        "Tag_" + orderId /* tag */,
                        "KEY" + i /* keys */,
                        ("Hello RocketMQ! order_" + orderId + " step_" + j).getBytes(RemotingHelper.DEFAULT_CHARSET) /* message body */
                );

                // 以orderId作为分区分类标准，对所有队列个数取余，来对将相同orderId的消息发送到同一个队列中。
                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    // mqs: 是 RocketMQ 集群环境中可以发送的队列（服务器上共有 8 个队列，两台服务，每台服务有 4 个队列）
                    // msg: 是消息，上述定义的 message
                    // arg: 是上述 send 接口中传入的第三个参数 Object 对象，也就是 orderId。也是通过该参数指定将消息存放在 mq 的哪个
                    //      queue 中。也可以通过遍历 mqs 指定一个特定的 queue 存放。
                    // 返回的是该消息需要发送到的队列
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        Integer id = (Integer) arg;
                        int index = id % mqs.size();
                        return mqs.get(index);
                    }
                }, orderId);

                System.out.printf("%s%n", sendResult);
            }
        }

        producer.shutdown();
    }
}
