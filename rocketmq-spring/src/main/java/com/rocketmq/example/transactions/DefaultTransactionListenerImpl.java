package com.rocketmq.example.transactions;

import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.springframework.messaging.Message;

/**
 * 业务的事务监听
 * 需要用到分布式事务的场景可以通过实现 RocketMQLocalTransactionListener 接口方法的方式完成
 * 默认 rocketMQTemplateBeanName 为 rocketMQTemplate，也可以制定自定义的 template
 */
@RocketMQTransactionListener
public class DefaultTransactionListenerImpl implements RocketMQLocalTransactionListener {

    /**
     * 本地事务方法，用于提交生产者上本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public RocketMQLocalTransactionState executeLocalTransaction(Message message, Object o) {
        return RocketMQLocalTransactionState.UNKNOWN;
    }

    /**
     * 校验本地食物方法，用于MQ回查本地事务状态
     * @param message
     * @return
     */
    @Override
    public RocketMQLocalTransactionState checkLocalTransaction(Message message) {
        return RocketMQLocalTransactionState.COMMIT;
    }
}
