package org.rocketmq.example.pullpush.pull;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.rocketmq.example.util.ConfigUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 主动往 RocketMQ 服务器上发送请求，获取消息队列进行消费
 */
public class PullConsumer {

    private static final String NAMESRV_ADDRS = new ConfigUtil().getClusterIps();
    private static final String PRODUCER_GROUP = "PullProducerTestGroup";
    private static final String TOPIC = "Topic-PullProducerMessage";
    // 记录消费消息队列的偏移量
    private static final Map<MessageQueue, Long> OFFSET_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(PRODUCER_GROUP);
        consumer.setNamesrvAddr(NAMESRV_ADDRS);
        consumer.start();

        // 获取某个Topic的所有队列，然后挑选队列进行拉取。
        Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues(TOPIC);
        for (MessageQueue message : messageQueueSet) {
            System.out.printf("Consume from the queue: %s%n", message);

            SINGLE_MQ:
            while (true) {
                try {
                    // MessageQueue: 消息队列
                    // subExpression: 指定 tag 过滤条件，这里指定*表示接收所有tag的消息
                    // offset: 消息队列的偏移量，指定偏移量后，将会从 queue 的该位置往下拉取消息
                    // maxNums: 一次最多从消息队列拉取消息的数量，最大设置为32
                    PullResult pullResult = consumer.pullBlockIfNotFound(message /* MessageQueue */,
                            "*" /* subExpression */,
                            getMessageQueueOffset(message) /* offset */,
                            32 /* maxNums */);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(message, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (MQBrokerException e) {
                    throw new RuntimeException(e);
                } catch (RemotingException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        consumer.shutdown();
    }

    /**
     * 获取消费点位（偏移量）
     *
     * @param mq
     * @return
     */
    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSET_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSET_TABLE.put(mq, offset);
    }
}
