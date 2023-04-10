package org.rocketmq.example.ordinarymessage.async;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.rocketmq.example.util.ConfigUtil;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步发送是指发送方发出一条消息后，不等服务端返回响应，接着发送下一条消息的通讯方式。
 *
 * 消息发送方在发送了一条消息后，不需要等待服务端响应即可发送第二条消息，发送方通过回调接口接收服务端响应，并处理响应结果。异步发送一般用于链路耗
 * 时较长，对响应时间较为敏感的业务场景。例如，视频上传后通知启动转码服务，转码完成后通知推送转码结果等。
 */
public class ProducerAsync {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "ProducerAsyncTestGroup";
    private static final String TOPIC = "Topic-ProducerAsyncMessage";

    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException,
            UnsupportedEncodingException, MQBrokerException {
        // 初始化一个 producer 并设置 Producer group name
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        // 设置 NameServer 地址
        producer.setNamesrvAddr(NAMESRV_ADDRS);
        // 设置超时时间，消息默认发送的超时时间为3秒，一旦响应时间超过该超时时间将会报错
        producer.setSendMsgTimeout(10000);
        // 启动 producer
        producer.start();

        int messageCount = 20;
        //
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        for (int i = 0; i < messageCount; i++) {
            final int index = i;
            // 创建一条消息，并指定 topic、tag、body 等信息，tag 可以理解成标签，对消息进行再归类，RocketMQ 可以在消费端对 tag 进行过滤
            Message msg = new Message(TOPIC,
                    "TagB",
                    "Hello RocketMQ!".getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 异步发送消息, 发送结果通过 Callback 返回给客户端
            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }
        // 等待时长设置长一些，不然关闭 producer 后会报超时的异常
        countDownLatch.await(15, TimeUnit.SECONDS);
        System.out.println("--- --- 消息成功发送 --- ---");
        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }
}
