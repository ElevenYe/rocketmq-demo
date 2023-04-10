package org.rocketmq.example.ordinarymessage.sync;

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
 * 同步发送是最常用的方式，是指消息发送方发出一条消息后，会在收到服务端同步响应之后才发下一条消息的通讯方式，可靠的同步传输被广泛应用于各种场景，
 * 如重要的通知消息、短消息通知等。
 */
public class ProducerSync {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerSyncTestGroup";
    private static final String TOPIC = "Topic-ProducerSyncMessage";

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
        for (int i = 0; i < 20; i++) {
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message(TOPIC /* Topic */,
                    "TagA" /* Tag */,
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