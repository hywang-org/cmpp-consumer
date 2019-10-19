package com.i.server;

import com.i.server.consts.RedisConsts;
import com.i.server.dao.redis.*;
import com.i.server.data.mysql.dao.SmsDao;
import com.i.server.data.mysql.entity.Channel;
import com.i.server.handler.MessageReceiveHandler;
import com.i.server.rabbitmq.service.RabbitmqService;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.common.util.MsgId;
import com.zx.sms.connect.manager.EndpointConnector;
import com.zx.sms.connect.manager.EndpointEntity.SupportLongMessage;
import com.zx.sms.connect.manager.EndpointManager;
import com.zx.sms.connect.manager.cmpp.CMPPClientEndpointEntity;
import com.zx.sms.handler.api.BusinessHandlerInterface;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EchoServer {
	// private final int port;
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private final EndpointManager manager = EndpointManager.INS;

	@Autowired
	private RabbitmqService rabbitmqService;

	@Resource
	private SmsDao smsDao;

	@PostConstruct
	public void EchoServer() {
		connect();
	}

	@Resource
	AppInfoRedis r1;

	@Resource
	ChannelInfoRedis r2;

	@Resource
	ProducerRedis r3;

	@Resource
	ConsumerRedis r4;


	public void connect() {
		List<Channel> channelList = smsDao.find("from Channel");
		System.out.println("channelList size = " + channelList.size());
		for (Channel channel : channelList) {
			CMPPClientEndpointEntity client = new CMPPClientEndpointEntity();
			client.setId(channel.getSpId());
			client.setHost(channel.getSpIp());
			client.setPort(channel.getSpPort());
//			client.setUserName(channel.getSpLoginName());
			client.setUserName(channel.getSpId());
			client.setPassword(channel.getSpLoginPwd());
			client.setServiceId("");
			client.setMaxChannels((short) 1);
			client.setVersion((short) 0x20);
			client.setWriteLimit(channel.getSpeedLimit());

//			CMPPClientEndpointEntity client = new CMPPClientEndpointEntity();
//			client.setId("109002");
//			client.setHost("121.41.46.165");
//			client.setPort(7890);
//			client.setUserName("109002");
//			client.setPassword("Aa123456");
//			client.setServiceId("");
//			client.setMaxChannels((short) 1);
//			client.setVersion((short) 0x20);
//			client.setWriteLimit(100);

//			CMPPClientEndpointEntity client = new CMPPClientEndpointEntity();
//			client.setId("109010");
//			client.setHost("121.41.46.165");
//			client.setPort(7890);
//			client.setUserName("109010");
//			client.setPassword("Aa123456");
//			client.setServiceId("");
//			client.setMaxChannels((short) 1);
//			client.setVersion((short) 0x20);
//			client.setWriteLimit(100);

			client.setGroupName("test");
			client.setChartset(Charset.forName("utf-8"));
			client.setRetryWaitTimeSec((short) 30);
			client.setUseSSL(false);
			client.setMaxRetryCnt((short) 0);
			client.setReSendFailMsg(false);
			client.setSupportLongmsg(SupportLongMessage.BOTH);
			List<BusinessHandlerInterface> clienthandlers = new ArrayList<BusinessHandlerInterface>();
			clienthandlers.add(new MessageReceiveHandler(rabbitmqService, smsDao));
			// clienthandlers.add( new SessionConnectedHandler());
			client.setBusinessHandlerSet(clienthandlers);

			manager.addEndpointEntity(client);

			for (int i = 0; i < client.getMaxChannels(); i++) {
				manager.openEndpoint(client);
			}
		}

		Map<String, RedisOperationSets> redisOperationSetsMap = new HashMap<String, RedisOperationSets>();
		redisOperationSetsMap.put(RedisConsts.REDIS_APP_INFO, r1);
		redisOperationSetsMap.put(RedisConsts.REDIS_CHANNEL_INFO, r2);
		redisOperationSetsMap.put(RedisConsts.REDIS_PRODUCER, r3);
		redisOperationSetsMap.put(RedisConsts.REDIS_CONSUMER, r4);
		// ly modify
		manager.setRedisOperationSetsMap(redisOperationSetsMap);

		System.out.println("hi = " + manager.getRedisOperationSetsMap().get(RedisConsts.REDIS_CHANNEL_INFO).getRedisTemplate().opsForHash().get("109002", "sp_ip"));
//		try {
//			Thread.sleep(6000);
//		} catch (Exception e) {
//
//		}
//		sendSms("13966732101", "【哈工大机器人】我一三", "109010");
	}

	public static void sendSms2(CmppSubmitRequestMessage msg) {
		EndpointConnector<?> connector = EndpointManager.INS.getEndpointConnector("109002");
		while (true) {
			ChannelFuture write = connector.asynwrite(msg);
			if (write != null) {
				break;
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		;
	}

	public static void sendSms(String mobile, String content, String id) {
		CmppSubmitRequestMessage msg = new CmppSubmitRequestMessage();
		msg.setDestterminalId(mobile);
		msg.setLinkID("0000");
		msg.setMsgContent(content);
		msg.setRegisteredDelivery((short) 1);
		msg.setMsgid(new MsgId());
		msg.setServiceId("");
		msg.setSrcId("10690427969010");
//		msg.setSrcId("106909009002");
		msg.setMsgsrc(id);
		EndpointConnector<?> connector = EndpointManager.INS.getEndpointConnector(id);
		while (true) {
			ChannelFuture write = connector.asynwrite(msg);
			System.out.println("written");
			if (write != null) {
				break;
			} else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		;
	}
}