package com.rocketmq.example.controller;

import com.rocketmq.example.producers.SpringProducer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
@RequestMapping("/mqTest")
public class MQTestController {

    private final String TOPIC = "Topic-SpringMQTest";

    @Resource
    SpringProducer springProducer;

    @RequestMapping("/sendMessage")
    public String sendMessage(String message) {
        springProducer.sendMessage(TOPIC, message);
        return "消息发送成功";
    }

    @RequestMapping("/sendTransactionMessage")
    public String sendTransactionMessage(String message) {
        springProducer.sendMessageInTransaction(TOPIC, message);
        return "消息发送成功";
    }
}
