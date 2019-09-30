package com.i.server;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.i.server.consts.RedisConsts;
import com.i.server.dao.redis.ConsumerRedis;
import com.i.server.dao.redis.MsgIdAppIdRedis;
import com.i.server.dao.redis.ProducerRedis;
import com.i.server.dao.redis.RedisOperationSets;
import com.i.server.dao.redis.ValidateClientRedis;
import com.i.server.handler.MessageReceiveHandler1;
import com.i.server.rabbitmq.service.RabbitmqService;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.common.util.MsgId;
import com.zx.sms.connect.manager.EndpointConnector;
import com.zx.sms.connect.manager.EndpointEntity.SupportLongMessage;
import com.zx.sms.connect.manager.EndpointManager;
import com.zx.sms.connect.manager.cmpp.CMPPClientEndpointEntity;
import com.zx.sms.handler.api.BusinessHandlerInterface;

import io.netty.channel.ChannelFuture;

@Service
public class EchoServer {
	// private final int port;
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private final EndpointManager manager = EndpointManager.INS;

	@Autowired
	private RabbitmqService rabbitmqService;

	@PostConstruct
	public void EchoServer() {
		connect();
	}

	@Resource
	ValidateClientRedis r1;

	@Resource
	MsgIdAppIdRedis r2;

	@Resource
	ConsumerRedis r3;

	@Resource
	ProducerRedis r4;

	public void connect() {
		CMPPClientEndpointEntity client = new CMPPClientEndpointEntity();
		client.setId("109002");
		client.setHost("121.41.46.165");
		client.setPort(7890);
		client.setUserName("109002");
		client.setPassword("Aa123456");
		client.setServiceId("");
		client.setMaxChannels((short) 1);
		client.setVersion((short) 0x20);
		client.setWriteLimit(100);

		client.setGroupName("test");
		client.setChartset(Charset.forName("utf-8"));
		client.setRetryWaitTimeSec((short) 30);
		client.setUseSSL(false);
		client.setMaxRetryCnt((short) 0);
		client.setReSendFailMsg(false);
		client.setSupportLongmsg(SupportLongMessage.BOTH);
		List<BusinessHandlerInterface> clienthandlers = new ArrayList<BusinessHandlerInterface>();
		clienthandlers.add(new MessageReceiveHandler1(rabbitmqService));
		// clienthandlers.add( new SessionConnectedHandler());
		client.setBusinessHandlerSet(clienthandlers);

		Map<String, RedisOperationSets> redisOperationSetsMap = new HashMap<String, RedisOperationSets>();
		redisOperationSetsMap.put(RedisConsts.REDIS_VALIDATE_CLINET, r1);
		redisOperationSetsMap.put(RedisConsts.REDIS_MSGID_APPID_INFO, r2);
		redisOperationSetsMap.put(RedisConsts.REDIS_CONSUMER, r3);
		redisOperationSetsMap.put(RedisConsts.REDIS_PRODUCER, r4);
		// ly modify
		manager.setRedisOperationSetsMap(redisOperationSetsMap);

		manager.addEndpointEntity(client);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < client.getMaxChannels(); i++)
			manager.openEndpoint(client);
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
		msg.setSrcId("106909009002");
		msg.setMsgsrc("109002");
		EndpointConnector<?> connector = EndpointManager.INS.getEndpointConnector(id);
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
}