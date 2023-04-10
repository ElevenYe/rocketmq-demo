package org.rocketmq.example.ordinarymessage.oneway;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.rocketmq.example.util.ConfigUtil;

import java.io.UnsupportedEncodingException;

/**
 * 单向模式调用sendOneway，不会对返回结果有任何等待和处理。故没有重试机制。
 *
 * 发送方只负责发送消息，不等待服务端返回响应且没有回调函数触发，即只发送请求不等待应答。此方式发送消息的过程耗时非常短，一般在微秒级别。适用于某
 * 些耗时非常短，但对可靠性要求并不高的场景，例如日志收集。
 */
public class ProducerOneway {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerOnewayTestGroup";
    private static final String TOPIC = "Topic-ProducerOnewayMessage";

    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, RemotingException,
            InterruptedException {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        // 设置NameServer地址
        producer.setNamesrvAddr(NAMESRV_ADDRS);
        // 设置超时时间，消息默认发送的超时时间为3秒，一旦响应时间超过该超时时间将会报错
        producer.setSendMsgTimeout(10000);
        // 启动producer
        producer.start();

        for (int i = 0; i < 20; i++) {
            // // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message(TOPIC /* Topic */,
                    "TagA"  /* Tag */,
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 由于在oneway方式发送消息时没有请求应答处理，如果出现消息发送失败，则会因为没有重试而导致数据丢失。若数据不可丢，建议选用可靠同
            // 步或可靠异步发送方式。
            producer.sendOneway(msg);
        }
        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }
}
