package org.rocketmq.example.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.rocketmq.example.util.ConfigUtil;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 分布式事务-消息提供者
 */
public class TransactionProducer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "TransactionProducerTestGroup";
    private static final String TOPIC = "Topic-TransactionProducerMessage";

    public static void main(String[] args) throws MQClientException, InterruptedException {
        // 创建事务回查的线程池
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        // 事务消息的发送不再使用 DefaultMQProducer，而是使用 TransactionMQProducer 进行发送
        TransactionMQProducer producer = new TransactionMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(NAMESRV_ADDRS);
        // 设置事务回查的线程池，如果不设置也会默认生成一个
        producer.setExecutorService(executorService);
        // 设置事务的监听。实现本地事务和回查本地事务状态的类
        producer.setTransactionListener(new TransactionListenerImpl());
        // 设置超时时间，消息默认发送的超时时间为3秒，一旦响应时间超过该超时时间将会报错
        producer.setSendMsgTimeout(10000);
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC"};
        for (int i = 0; i < 3; i++) {
            try {
                // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
                Message msg = new Message(TOPIC /* Topic */,
                        tags[i % tags.length] /* Tag */,
                        "KEY" + i /* keys */,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
                // 发送事务消息
                // arg：用于指定在执行本地事务时要使用的业务参数
                SendResult sendResult = producer.sendMessageInTransaction(msg, "你好啊！");
                System.out.printf("%s %s%n", sendResult, msg);

                Thread.sleep(1000);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
