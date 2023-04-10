package org.rocketmq.example.filtermessage.tagfilter;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.rocketmq.example.util.ConfigUtil;

import java.io.UnsupportedEncodingException;

/**
 * Tag消息过滤-消息生产者
 * 消息过滤是指消息生产者向Topic中发送消息时，设置消息属性对消息进行分类，消费者订阅Topic时，根据消息属性设置过滤条件对消息进行过滤，只有符合过
 * 滤条件的消息才会被投递到消费端进行消费。
 *
 * 消费者订阅Topic时若未设置过滤条件，无论消息发送时是否有设置过滤属性，Topic中的所有消息都将被投递到消费端进行消费。
 */
public class TagFilterProducer {
    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerTagFilterTestGroup";
    private static final String TOPIC = "Topic-ProducerTagFilterMessage";

    public static void main(String[] args) throws MQBrokerException, RemotingException, InterruptedException,
            MQClientException, UnsupportedEncodingException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP); //（1）
        // 设置NameServer地址
        producer.setNamesrvAddr(NAMESRV_ADDRS);  //（2）
        // 设置超时时间，消息默认发送的超时时间为3秒，一旦响应时间超过该超时时间将会报错
        producer.setSendMsgTimeout(10000);
        // 启动producer
        producer.start();

        String[] tags = {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 15; i++) {
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message(TOPIC /* Topic */,
                    tags[i % tags.length] /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );   //（3）
            // 利用producer进行发送，并同步等待发送结果
            SendResult sendResult = producer.send(msg);   //（4）
            System.out.printf("%s%n", sendResult);
        }
        // 一旦producer不再使用，关闭producer(注意：一旦关闭producer，对应的生产者组"ProducerSyncTestGroup"也会被删掉)
        producer.shutdown();
        System.out.println("--- --- 消息发送成功！ --- ---");
    }
}
