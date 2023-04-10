package com.rocketmq.example.transactions;

import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 业务的事务监听
 * 需要用到分布式事务的场景可以通过实现 RocketMQLocalTransactionListener 接口方法的方式完成
 * 默认 rocketMQTemplateBeanName 为 rocketMQTemplate，也可以制定自定义的 template
 */
@RocketMQTransactionListener(rocketMQTemplateBeanName = "businessExtRocketMQTemplate")
public class BusinessTransactionListenerImpl implements RocketMQLocalTransactionListener {

    // 事务的索引值
    private AtomicInteger transactionIndex = new AtomicInteger(0);

    // 存储事务状态
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<String, Integer>();

    // 存储不同消息的消息回查次数
    private ConcurrentHashMap<String, AtomicInteger> checkCntMap = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * 本地事务方法，用于提交生产者上本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
        String transId = message.getHeaders().get(RocketMQHeaders.PREFIX + RocketMQHeaders.TRANSACTION_ID).toString();
        System.out.println(o);
        // 索引值原子 +1
        int value = transactionIndex.getAndIncrement();
        // 根据索引值计算事务状态
        int status = value % 3;
        localTrans.put(transId, status);
        System.out.printf("%s_%s_", transId, status);
        // 模拟本地事物状态为"未知"，当 mq 服务收到消息提供者发送的事务状态为"未知"时将会触发消息回查，重新向消息提供者确认事务状态。
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * 校验本地食物方法，用于MQ回查本地事务状态
     * @param message
     * @return
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        String transId = message.getHeaders().get(RocketMQHeaders.PREFIX + RocketMQHeaders.TRANSACTION_ID).toString();
        int status = localTrans.get(transId);
        System.out.printf("%s: %s %s%n", transId, status, localTrans);
        // 模拟本地部分事务 commit、rollback 和服务宕机的情况
        switch (status) {
            case 0:
                if (!checkCntMap.containsKey(transId)) {
                    checkCntMap.put(transId, new AtomicInteger(1));
                } else {
                    checkCntMap.get(transId).getAndIncrement();
                }
                System.out.printf("%s MQ服务器消息第%s次回查：%s %n", transId,
                        checkCntMap.get(transId), message);
                return RocketMQLocalTransactionState.UNKNOWN; // "未知"：MQ 服务接受到此状态，MQ 服务将会再次进行消息回查。
            case 1:
                return RocketMQLocalTransactionState.COMMIT; // "已提交"：MQ 服务接受到此状态，将半事务消息标记为可投递，并投递给消费者。
            case 2:
                return RocketMQLocalTransactionState.ROLLBACK; // "实物回滚"：MQ 服务接收到此状态，就不会将半事务消息投递给消费者。
            default:
                return RocketMQLocalTransactionState.COMMIT;
        }
    }
}
