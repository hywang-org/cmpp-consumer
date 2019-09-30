package com.i.server.rabbitmq.consumer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.i.server.rabbitmq.consts.RabbitMqConsts;
import com.i.server.rabbitmq.service.RabbitmqService;
import com.i.server.util.QueueUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;

@Component
public class GetQueueAndCreateConsumer implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetQueueAndCreateConsumer.class);

    @Autowired
    private RabbitmqService rabbitmqService;

    // @Autowired
    // private RedisService redisService;
    //
    // @Autowired
    // private APIService apiService;
    //
    // @Autowired
    // private YPDao ypDao;

    @Autowired
    private QueueUtils queueUtils;

    @Value("${mq_waitTime}")
    private long waitTime;

    @Value("${numberOfConsumer}")
    private int numberOfConsumer;

    public void createConsumer() throws IOException, TimeoutException {

        // 获取rabbitMq服务器中已存在的队列名
        List<String> consumerWatiToCreate = queueUtils.getQueueNameList();
        if (consumerWatiToCreate != null && !consumerWatiToCreate.isEmpty()) {
            for (String queueName : consumerWatiToCreate) {
                if (queueName.startsWith(RabbitMqConsts.NETTY_APPID_QUEUE_NAME_PREFIX)) {
                    for (int i = 0; i < numberOfConsumer; i++) {
                        Channel channel = rabbitmqService.getChannel();
                        channel.confirmSelect();
                        channel.basicQos(1);
                        Consumer consumer = new AppConvertConsumer(channel);
                        channel.basicConsume(queueName, false, consumer);
                        LOGGER.info("容器启动时，APP用户在RabbitMq中已经存在的队列{}对应的Consumer创建成功", queueName);
                    }
                }

            }
        }
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        try {
            createConsumer();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
