package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import com.alibaba.fastjson.parser.ParserConfig;
import com.i.server.rabbitmq.consumer.CreateQueueAndConsumerByMq;
import com.i.server.rabbitmq.consumer.GetQueueAndCreateConsumer;

@SpringBootApplication
public class NettyConsumer {

	private static ConfigurableApplicationContext context;

	public static ConfigurableApplicationContext getContext() {
		return context;
	}

	public static void setContext(ConfigurableApplicationContext context) {
		NettyConsumer.context = context;
	}

	public static void main(String[] args) throws Exception {
		ParserConfig.getGlobalInstance().setAutoTypeSupport(true);
		ConfigurableApplicationContext context = SpringApplication.run(NettyConsumer.class, args);
		setContext(context);
		context.addApplicationListener(new GetQueueAndCreateConsumer());
		context.addApplicationListener(new CreateQueueAndConsumerByMq());
	}

}
