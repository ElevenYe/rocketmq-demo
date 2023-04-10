package org.rocketmq.example.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 实现本地事务和回查本地事务状态的类
 * 需要实现 TransactionListener 接口，并传入 TransactionMQProducer。
 */
public class TransactionListenerImpl implements TransactionListener {
    // 事务的索引值
    private AtomicInteger transactionIndex = new AtomicInteger(0);

    // 存储事务状态
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<String, Integer>();

    // 存储不同消息的消息回查次数
    private ConcurrentHashMap<String, AtomicInteger> checkCntMap = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * 回调操作，执行本地事务
     * 消息预提交成功就会出发该方法的执行
     * @param msg
     * @param arg
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println(arg);
        // 索引值原子 +1
        int value = transactionIndex.getAndIncrement();
        // 根据索引值计算事务状态
        int status = value % 3;
        localTrans.put(msg.getTransactionId(), status);
        System.out.printf("%s_%s_", msg.getTransactionId(), status);
        // 模拟本地事物状态为"未知"，当 mq 服务收到消息提供者发送的事务状态为"未知"时将会触发消息回查，重新向消息提供者确认事务状态。
        return LocalTransactionState.UNKNOW;
    }

    /**
     * 用于 MQ 回查消息时确认本地事务是否已提交
     * @param msg
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        int status = localTrans.get(msg.getTransactionId());
        System.out.printf("%s: %s %s%n", msg.getTransactionId(), status, localTrans);
        // 模拟本地部分事务 commit、rollback 和服务宕机的情况
        switch (status) {
            case 0:
                if (!checkCntMap.containsKey(msg.getTransactionId())) {
                    checkCntMap.put(msg.getTransactionId(), new AtomicInteger(1));
                } else {
                    checkCntMap.get(msg.getTransactionId()).getAndIncrement();
                }
                System.out.printf("%s MQ服务器消息第%s次回查：%s %n", msg.getTransactionId(),
                        checkCntMap.get(msg.getTransactionId()), msg);
                return LocalTransactionState.UNKNOW; // "未知"：MQ 服务接受到此状态，MQ 服务将会再次进行消息回查。
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE; // "已提交"：MQ 服务接受到此状态，将半事务消息标记为可投递，并投递给消费者。
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE; // "实物回滚"：MQ 服务接收到此状态，就不会将半事务消息投递给消费者。
            default:
                return LocalTransactionState.COMMIT_MESSAGE;
        }
    }
}