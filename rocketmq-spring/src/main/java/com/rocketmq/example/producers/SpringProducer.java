package com.rocketmq.example.producers;

import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class SpringProducer {

    @Resource
    private RocketMQTemplate rocketMQTemplate;

    public void sendMessage(String topic, String msg) {
        rocketMQTemplate.convertAndSend(topic, msg);
    }

    public void sendMessageInTransaction(String topic, String msg) {
        String[] tags = new String[] {"TagA, TagB, TagC, TagD, TagE"};

        for (int i = 0; i < 10; i++) {
            // 尝试在 message 中加一些自定义属性
            Message<String> message = MessageBuilder.withPayload(msg)
                    //.setHeader(RocketMQHeaders.TRANSACTION_ID, "TransID_" + i)
                    .setHeader(RocketMQHeaders.TAGS, tags[i % tags.length])
                    .setHeader(RocketMQHeaders.KEYS, "Key_" + i)
                    .setHeader("MyProp", "MyProp_" + i)
                    .build();

            String destination = topic + ":" + tags[i % tags.length];

            TransactionSendResult sendResult = rocketMQTemplate.sendMessageInTransaction(destination, message, destination);
            System.out.printf("%s %s%n", sendResult, msg);
        }
    }
}
