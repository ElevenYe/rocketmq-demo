package org.rocketmq.example.delaymessage;

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
 * 延迟消息发送-消息提供者
 *
 * 在 apache 开源版本中支持 18 个等级的延迟消息发送，无法支持定制延迟时间。只有阿里的商业版本才支持定制延迟时间。
 * 因此在使用 apache 开源版作为生产 RocketMQ，并且有定制延迟时间发送消息需求的情况下，msg.setDelayTimeLevel 是开发人员修改源码实现定制化
 * 的重点改造任务
 */
public class DelayProducer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerDelayTestGroup";
    private static final String TOPIC = "Topic-ProducerDelayMessage";

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
        for (int i = 0; i < 2; i++) {
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message(TOPIC /* Topic */,
                    "TagA" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );   //（3）
            // 此处设置消息延迟投递，延迟投递共有18个等级
            // 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            // 以下设置 3 对应上述 10s 后进行投递
            msg.setDelayTimeLevel(3);
            // 利用producer进行发送，并同步等待发送结果
            SendResult sendResult = producer.send(msg);   //（4）
            System.out.printf("%s%n", sendResult);
        }
        // 一旦producer不再使用，关闭producer(注意：一旦关闭producer，对应的生产者组"ProducerSyncTestGroup"也会被删掉)
        producer.shutdown();
        System.out.println("--- --- 消息发送成功！ --- ---");
    }
}
