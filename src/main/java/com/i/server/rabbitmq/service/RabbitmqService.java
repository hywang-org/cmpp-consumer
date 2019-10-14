package com.i.server.rabbitmq.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.i.server.consts.Consts;
import com.i.server.rabbitmq.consts.RabbitMqConsts;
import com.i.server.util.QueueUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.zx.sms.codec.cmpp.msg.CmppDeliverRequestMessage;
import com.zx.sms.codec.cmpp.msg.CmppDeliverRequestSelfDefinedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

@Service
public class RabbitmqService {

	private static final Logger LOGGER = LoggerFactory.getLogger(RabbitmqService.class);

	@Value("${mq_username}")
	private String mqUserName;

	@Value("${mq_password}")
	private String mqPassWord;

	@Value("${mq_host}")
	private String mqHost;

	@Autowired
	private QueueUtils queueUtils;

	private Connection connection = null;

	public Connection getConnection() throws IOException, TimeoutException {
		if (connection == null || !connection.isOpen()) {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setUsername(mqUserName);
			factory.setPassword(mqPassWord);
			factory.setHost(mqHost);
			factory.setAutomaticRecoveryEnabled(true); // 设置网络异常重连
			factory.setNetworkRecoveryInterval(10000);// 设置每10s重试一次
			factory.setTopologyRecoveryEnabled(true);// 设置重新声明交换器，队列等信息。
			connection = factory.newConnection();
		}
		return connection;
	}

	public Channel getChannel() throws IOException, TimeoutException {
		return getConnection().createChannel();
	}

	public void publishBackMsgToMq(String serverId, String appId, String ownMsgId, String channelId, long ownSequenceId, String cmppType,
	                               Object obj) throws IOException, TimeoutException {
		MqEntity mqEntity = null;
		switch (cmppType) {
			case Consts.CMPP_DELIVER_REQUEST_MESSAGE:
				CmppDeliverRequestSelfDefinedMessage cmppDeliverRequestSelfDefinedMessage = ((CmppDeliverRequestMessage) obj)
						.getCmppDeliverRequestSelfDefinedMessage();
				System.out.println("getBodyBuffer length = " + cmppDeliverRequestSelfDefinedMessage.getBodyBuffer().length);
				mqEntity = new MqEntity();
				mqEntity.setAppId(appId);
				mqEntity.setChannelId(channelId);
				mqEntity.setOwnSequenceId(ownSequenceId);
				mqEntity.setMsgId(((CmppDeliverRequestMessage) obj).getMsgId());
				mqEntity.setObj(cmppDeliverRequestSelfDefinedMessage);
				mqEntity.setCmppMsgType(cmppType);
				mqEntity.setCmppVersion(((CmppDeliverRequestMessage) obj).getCmppVersion());
				break;
			default:
				LOGGER.error("wrong cmpp type");
				break;
		}

		if (mqEntity != null) {
			Channel channel = getConnection().createChannel();
			channel.confirmSelect();
			// 声明create_queue和create_consumer
			channel.basicQos(1);
			String queueName = RabbitMqConsts.NETTY_APPID_BACK_QUEUE_NAME_PREFIX + appId + "_" + serverId;
			String exchangeName = RabbitMqConsts.NETTY_APPID_BACK_EXCHANGE_NAME_PREFIX + appId + "_" + serverId;
			if (!isQueueExist(queueName)) {
				channel.exchangeDeclare(exchangeName, "direct", true);
				channel.queueDeclare(queueName, true, false, false, null);
				// 对队列进行绑定
				channel.queueBind(queueName, exchangeName, "consume");
				LOGGER.info("普通用户userId={}的转写任务队列{}创建成功", appId, queueName);
				// 发布到create_queue创建对应的consumer
				channel.basicPublish(RabbitMqConsts.NETTY_CREATE_BACK_QUEUE_EXCHANGE_NAME + "_" + serverId, "create",
						MessageProperties.PERSISTENT_TEXT_PLAIN, queueName.getBytes());
			}
			channel.basicPublish(exchangeName, "consume", MessageProperties.PERSISTENT_TEXT_PLAIN,
					JSON.toJSONBytes(mqEntity, SerializerFeature.WriteClassName));
			LOGGER.info("publishBackMsgToMq 普通用户appId={}的数据msgId={}发送到消息队列，消息体为{}", appId, mqEntity.getMsgId(),
					JSON.toJSONString(mqEntity));

			channel.close();
		} else {
			LOGGER.info("Not pulish to mq due to empty MqEntity object");
		}
	}

	/**
	 * 判断当前MQ中是否存在userId或者appId对应的queue 若存在则返回true，不存在返回false，并把queueName加到集合中
	 *
	 * @param queueName
	 * @return
	 */
	private boolean isQueueExist(String queueName) {
		Set<String> queueSet = queueUtils.getQueueNameSet();
		if (!queueSet.contains(queueName)) {
			queueSet.add(queueName);
			return false;
		}
		return true;
	}

}
