package com.rocketmq.example.config;

import org.apache.rocketmq.spring.annotation.ExtRocketMQTemplateConfiguration;
import org.apache.rocketmq.spring.core.RocketMQTemplate;

/**
 * 自定义 MQTemplate
 */
@ExtRocketMQTemplateConfiguration
public class BusinessExtRocketMQTemplate extends RocketMQTemplate {
}
