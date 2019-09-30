package com.i.server.handler;

import com.alibaba.fastjson.JSONObject;
import com.i.server.consts.Consts;
import com.i.server.consts.RedisConsts;
import com.i.server.rabbitmq.service.RabbitmqService;
import com.zx.sms.codec.cmpp.msg.CmppDeliverRequestMessage;
import com.zx.sms.codec.cmpp.msg.CmppDeliverResponseMessage;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.codec.cmpp.msg.CmppSubmitResponseMessage;
import com.zx.sms.common.util.MsgId;
import com.zx.sms.connect.manager.EndpointManager;
import com.zx.sms.handler.api.AbstractBusinessHandler;
import com.zx.sms.session.cmpp.SessionState;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;

@Sharable
public class MessageReceiveHandler1 extends AbstractBusinessHandler {

	private final EndpointManager manager = EndpointManager.INS;

	private RabbitmqService rabbitmqService;

	public MessageReceiveHandler1(RabbitmqService rabbitmqService) {
		this.rabbitmqService = rabbitmqService;
	}

	@Override
	public String name() {
		return "MessageReceiveHandler-smsBiz";
	}

	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt == SessionState.Connect) {
			System.out.println("userEventTriggered connect");
		}
		ctx.fireUserEventTriggered(evt);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		// System.out.println(msg);
		if (msg instanceof CmppDeliverRequestMessage) {
			CmppDeliverRequestMessage e = (CmppDeliverRequestMessage) msg;
			System.out.println("!!! " + e.getCmppVersion());
			String spMsgId = String.valueOf(e.getReportRequestMessage().getMsgId());
			long ownSequenceId = Long
					.valueOf(manager.getRedisOperationSetsMap().get(RedisConsts.REDIS_CONSUMER).get(spMsgId));
			String jsonString = manager.getRedisOperationSetsMap().get(RedisConsts.REDIS_PRODUCER)
					.get(String.valueOf(ownSequenceId));
			JSONObject jsonObject1 = JSONObject.parseObject(jsonString);
			System.out.println("CmppDeliverRequestMessage spMsgId = " + spMsgId + ", ownSequenceId = " + ownSequenceId);
			System.out.println("CmppDeliverRequestMessage json = " + jsonString);
			String appId = jsonObject1.getString("appId");
			String ownMsgId = jsonObject1.getString("ownMsgId");
			String channelId = jsonObject1.getString("channelId");
			Long clientSequenceId = jsonObject1.getLong("clientSequenceId");

			System.out.println("MessageReceiveHandler1 CmppDeliverRequestMessage:" + e);
			CmppDeliverResponseMessage responseMessage = new CmppDeliverResponseMessage(e.getHeader().getSequenceId());
			responseMessage.setResult(0);

			e.setSequenceNo(clientSequenceId);
			e.setMsgId(new MsgId(ownMsgId));
			rabbitmqService.publishBackMsgToMq(appId, ownMsgId, channelId, ownSequenceId,
					Consts.CMPP_DELIVER_REQUEST_MESSAGE, e);
			ctx.channel().writeAndFlush(responseMessage);
			// cnt.incrementAndGet();

		} else if (msg instanceof CmppDeliverResponseMessage) {
			CmppDeliverResponseMessage e = (CmppDeliverResponseMessage) msg;
			System.out.println("MessageReceiveHandler1 CmppDeliverResponseMessage :" + e);
		} else if (msg instanceof CmppSubmitRequestMessage) {
			CmppSubmitRequestMessage e = (CmppSubmitRequestMessage) msg;
			// CmppSubmitResponseMessage resp = new
			// CmppSubmitResponseMessage(e.getHeader().getSequenceId());
			// // resp.setResult(RandomUtils.nextInt()%1000 <10 ? 8 : 0);
			// System.out.println("MessageReceiveHandler1
			// CmppSubmitRequestMessage :" + e);
			// resp.setResult(0);
			// ctx.channel().writeAndFlush(resp);
		} else if (msg instanceof CmppSubmitResponseMessage) {
			CmppSubmitResponseMessage e = (CmppSubmitResponseMessage) msg;
			String spMsgId = String.valueOf(e.getMsgId());
			long ownSequenceId = e.getSequenceNo();
			System.out.println("MessageReceiveHandler1 spMsgId = " + spMsgId + ", ownSequenceId = " + ownSequenceId);
			manager.getRedisOperationSetsMap().get(RedisConsts.REDIS_CONSUMER).set(spMsgId,
					String.valueOf(ownSequenceId));
			long result = e.getResult();
			if (result == 0) {
				System.out.println("提交成功");
			} else {
				System.out.println("提交失败");
			}
			System.out.println("MessageReceiveHandler1 CmppSubmitResponseMessage :" + e);
		} else {
			ctx.fireChannelRead(msg);
		}
	}

	public MessageReceiveHandler1 clone() throws CloneNotSupportedException {
		MessageReceiveHandler1 ret = (MessageReceiveHandler1) super.clone();
		return ret;
	}

}
