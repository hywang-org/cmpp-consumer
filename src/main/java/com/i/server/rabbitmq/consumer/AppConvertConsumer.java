package com.i.server.rabbitmq.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.i.server.consts.Consts;
import com.i.server.rabbitmq.service.MqEntity;
import com.i.server.rabbitmq.service.RabbitmqService;
import com.rabbitmq.client.*;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestMessage;
import com.zx.sms.codec.cmpp.msg.CmppSubmitRequestSelfDefinedMessage;
import com.zx.sms.codec.cmpp.packet.CmppSubmitRequest;
import com.zx.sms.codec.cmpp.wap.LongMessageFrameHolder;
import com.zx.sms.codec.cmpp20.packet.Cmpp20SubmitRequest;
import com.zx.sms.common.GlobalConstance;
import com.zx.sms.common.util.DefaultMsgIdUtil;
import com.zx.sms.common.util.MsgId;
import com.zx.sms.connect.manager.EndpointConnector;
import com.zx.sms.connect.manager.EndpointManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import org.marre.sms.SmsDcs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;

import static com.zx.sms.common.util.NettyByteBufUtil.toArray;

public class AppConvertConsumer extends DefaultConsumer {

	public AppConvertConsumer(Channel channel) {
		super(channel);
		// TODO Auto-generated constructor stub
	}

	private static final Logger LOGGER = LoggerFactory.getLogger(AppConvertConsumer.class);

	private RabbitmqService rabbitmqService;

	private String queueName;

	// private RedisService redisService;
	//
	// private APIService apiService;
	//
	// private YPDao ypDao;

	private long waitTime;

	// public AppConvertConsumer(RabbitmqService rabbitmqService, String
	// queueName, Channel channel,
	// RedisService redisService, APIService apiService, YPDao ypDao, long
	// waitTime,
	// DeleteOrderService deleteOrderService) {
	// super(channel);
	// this.rabbitmqService = rabbitmqService;
	// this.queueName = queueName;
	// this.redisService = redisService;
	// this.apiService = apiService;
	// this.ypDao = ypDao;
	// this.waitTime = waitTime;
	// this.deleteOrderService = deleteOrderService;
	// }

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
			throws IOException {
		// String test = JSONArray.toJSONString(body);
		String test = new String(body, "UTF-8");
		System.out.println("test = " + JSON.toJSONString(test));
		MqEntity mqEntity = JSON.parseObject(test, new TypeReference<MqEntity>() {
		});
		// System.out.println(
		// "test trim = " + test.replaceAll("\\\\", "").replaceAll("\"\\{",
		// "{").replaceAll("\\}\"", "}"));
		// MqEntity mqEntity = JSON.parseObject(
		// test.replaceAll("\\\\", "").replaceAll("\"\\{",
		// "{").replaceAll("\\}\"", "}"),
		// new TypeReference<MqEntity>() {
		// });
		MsgId msgId = mqEntity.getMsgId();
		String cmppMsgType = mqEntity.getCmppMsgType();
		String cmppVersion = mqEntity.getCmppVersion();
		String msgContent = mqEntity.getMsgContent();
		System.out.println("cmppVersion = " + cmppVersion);
		switch (cmppMsgType) {
			case Consts.CMPP_SUBMIT_REQUEST_MESSAGE:
				CmppSubmitRequestSelfDefinedMessage cmppSubmitRequestSelfDefinedMessage = (CmppSubmitRequestSelfDefinedMessage) mqEntity
						.getObj();
				CmppSubmitRequestMessage cmppObj = formCmppMessage(cmppSubmitRequestSelfDefinedMessage, cmppVersion, msgId,
						msgContent);
				System.out.println("cmppObj msgcontent = " + cmppObj.getMsgContent());
				sendSms(cmppObj);
//				EchoServer.sendSms("13966732101", cmppObj.getMsgContent(), "109002");
		}
//		this.getChannel().basicAck(envelope.getDeliveryTag(), false);
		// CmppSubmitRequestSelfDefinedMessage selfDefinedMessage =
		// mqEntity.getSelfDefinedMessage();
		// System.out.println("selfDefinedMessage =" +
		// selfDefinedMessage.getBodyBuffer().length);
		// System.out.println("mqEntity obj = " + JSON.toJSONString(mqEntity));
		// System.out.println("msgId obj = " + msgId);
		// System.out.println("selfDefinedMessage obj = " +
		// JSON.toJSONString(selfDefinedMessage));

		// cmppObj.setMsgsrc("109002");
		// cmppObj.setDestterminalId("16655169698");
		// cmppObj.setMsgContent("【易信科技】我是短信内容");

		// EchoServer.sendSms("16655169698", "【易信科技】我是短信内容", "109002");
		// System.out.println("cmppObj msgId obj = " + cmppObj.getMsgid());

	}

	private void sendSms(Object obj) {
		EndpointConnector<?> connector = EndpointManager.INS.getEndpointConnector("109002");
		while (true) {
			LOGGER.info("ready publish from consumer," + obj);
			ChannelFuture write = connector.asynwrite(obj);
			LOGGER.info("written");
			if (write != null) {
				break;
			} else {
				try {
					java.lang.Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		;
	}

	private CmppSubmitRequestMessage formCmppMessage(CmppSubmitRequestSelfDefinedMessage selfDefinedMessage,
	                                                 String cmppVersion, MsgId msgId, String msgContent) {
		CmppSubmitRequestMessage cmppSubmitRequestMessage = null;
		switch (cmppVersion) {
			case Consts.TYPE20:
				cmppSubmitRequestMessage = formCmppMessage20(selfDefinedMessage, msgId, msgContent);
				break;
			default:
				cmppSubmitRequestMessage = formCmppMessage30(selfDefinedMessage, msgId);
				break;
		}
		return cmppSubmitRequestMessage;
	}

	private CmppSubmitRequestMessage formCmppMessage30(CmppSubmitRequestSelfDefinedMessage selfDefinedMessage,
	                                                   MsgId msgId) {
		CmppSubmitRequestMessage requestMessage = new CmppSubmitRequestMessage(selfDefinedMessage.getHeader());

		ByteBuf bodyBuffer = Unpooled.wrappedBuffer(selfDefinedMessage.getBodyBuffer());

		// ly add
		// requestMessage.setMsgid(DefaultMsgIdUtil.bytes2MsgId(toArray(bodyBuffer,
		// CmppSubmitRequest.MSGID.getLength())));
		DefaultMsgIdUtil.bytes2MsgId(toArray(bodyBuffer, CmppSubmitRequest.MSGID.getLength()));
		requestMessage.setMsgid(msgId);

		requestMessage.setPktotal(bodyBuffer.readUnsignedByte());
		requestMessage.setPknumber(bodyBuffer.readUnsignedByte());

		bodyBuffer.readUnsignedByte();
		//发送给运营商时无论客户是否需要接收deliver消息，我们都需要运营商给deliver消息用于记录
		requestMessage.setRegisteredDelivery(Consts.YES_DELIVER);

		System.out.println("requestMessage get = " + requestMessage.getRegisteredDelivery());
		requestMessage.setMsglevel(bodyBuffer.readUnsignedByte());
		requestMessage.setServiceId(bodyBuffer
				.readCharSequence(CmppSubmitRequest.SERVICEID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());
		requestMessage.setFeeUserType(bodyBuffer.readUnsignedByte());

		requestMessage.setFeeterminalId(bodyBuffer
				.readCharSequence(CmppSubmitRequest.FEETERMINALID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setFeeterminaltype(bodyBuffer.readUnsignedByte());

		requestMessage.setTppid(bodyBuffer.readUnsignedByte());
		requestMessage.setTpudhi(bodyBuffer.readUnsignedByte());
		requestMessage.setMsgfmt(new SmsDcs((byte) bodyBuffer.readUnsignedByte()));

		requestMessage.setMsgsrc(bodyBuffer
				.readCharSequence(CmppSubmitRequest.MSGSRC.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setFeeType(bodyBuffer
				.readCharSequence(CmppSubmitRequest.FEETYPE.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setFeeCode(bodyBuffer
				.readCharSequence(CmppSubmitRequest.FEECODE.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setValIdTime(bodyBuffer
				.readCharSequence(CmppSubmitRequest.VALIDTIME.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setAtTime(bodyBuffer
				.readCharSequence(CmppSubmitRequest.ATTIME.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setSrcId(bodyBuffer
				.readCharSequence(CmppSubmitRequest.SRCID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		short destUsrtl = bodyBuffer.readUnsignedByte();
		String[] destTermId = new String[destUsrtl];
		for (int i = 0; i < destUsrtl; i++) {
			destTermId[i] = bodyBuffer.readCharSequence(CmppSubmitRequest.DESTTERMINALID.getLength(),
					GlobalConstance.defaultTransportCharset).toString().trim();
		}
		requestMessage.setDestterminalId(destTermId);

		requestMessage.setDestterminaltype(bodyBuffer.readUnsignedByte());

		short msgLength = (short) (LongMessageFrameHolder.getPayloadLength(requestMessage.getMsgfmt().getAlphabet(),
				bodyBuffer.readUnsignedByte()) & 0xffff);

		byte[] contentbytes = new byte[msgLength];
		bodyBuffer.readBytes(contentbytes);
		requestMessage.setMsgContentBytes(contentbytes);
		requestMessage.setMsgLength((short) msgLength);

		requestMessage.setLinkID(bodyBuffer
				.readCharSequence(CmppSubmitRequest.LINKID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());
		return requestMessage;
	}

	private CmppSubmitRequestMessage formCmppMessage20(CmppSubmitRequestSelfDefinedMessage selfDefinedMessage,
	                                                   MsgId msgId, String msgContent) {
		CmppSubmitRequestMessage requestMessage = new CmppSubmitRequestMessage(selfDefinedMessage.getHeader());

		System.out.println("selfDefinedMessage.getHeader() head length = "
				+ selfDefinedMessage.getHeader().getHeadLength() + ",selfDefinedMessage.getHeader() body length = "
				+ selfDefinedMessage.getHeader().getBodyLength() + ", selfDefinedMessage.getBodyBuffer() length = "
				+ selfDefinedMessage.getBodyBuffer().length);

		ByteBuf bodyBuffer = Unpooled.wrappedBuffer(selfDefinedMessage.getBodyBuffer());

		// ly add
		// requestMessage
		// .setMsgid(DefaultMsgIdUtil.bytes2MsgId(toArray(bodyBuffer,
		// Cmpp20SubmitRequest.MSGID.getLength())));
		DefaultMsgIdUtil.bytes2MsgId(toArray(bodyBuffer, Cmpp20SubmitRequest.MSGID.getLength()));
		requestMessage.setMsgid(msgId);

		requestMessage.setPktotal(bodyBuffer.readUnsignedByte());
		requestMessage.setPknumber(bodyBuffer.readUnsignedByte());
		System.out.println("requestMessage getPktotal = " + requestMessage.getPktotal() + ", getPknumber = "
				+ requestMessage.getPknumber());

		// requestMessage.setRegisteredDelivery(bodyBuffer.readUnsignedByte());
		bodyBuffer.readUnsignedByte();
		//发送给运营商时无论客户是否需要接收deliver消息，我们都需要运营商给deliver消息用于记录
		requestMessage.setRegisteredDelivery(Consts.YES_DELIVER);
		System.out.println("requestMessage get = " + requestMessage.getRegisteredDelivery());
		requestMessage.setMsglevel(bodyBuffer.readUnsignedByte());
		requestMessage.setServiceId(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.SERVICEID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());
		requestMessage.setFeeUserType(bodyBuffer.readUnsignedByte());

		requestMessage.setFeeterminalId(bodyBuffer.readCharSequence(Cmpp20SubmitRequest.FEETERMINALID.getLength(),
				GlobalConstance.defaultTransportCharset).toString().trim());
		// requestMessage.setFeeterminaltype(bodyBuffer.readUnsignedByte());//CMPP2.0
		// 无该字段 不进行编解码

		requestMessage.setTppid(bodyBuffer.readUnsignedByte());
		requestMessage.setTpudhi(bodyBuffer.readUnsignedByte());
		requestMessage.setMsgfmt(new SmsDcs((byte) bodyBuffer.readUnsignedByte()));

		requestMessage.setMsgsrc(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.MSGSRC.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setFeeType(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.FEETYPE.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setFeeCode(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.FEECODE.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setValIdTime(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.VALIDTIME.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setAtTime(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.ATTIME.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());

		requestMessage.setSrcId(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.SRCID.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());
		short destUsrtl = bodyBuffer.readUnsignedByte();
		String[] destTermId = new String[destUsrtl];
		for (int i = 0; i < destUsrtl; i++) {
			destTermId[i] = bodyBuffer.readCharSequence(Cmpp20SubmitRequest.DESTTERMINALID.getLength(),
					GlobalConstance.defaultTransportCharset).toString().trim();
		}
		requestMessage.setDestterminalId(destTermId);

		// requestMessage.setDestterminaltype(bodyBuffer.readUnsignedByte());//CMPP2.0
		// 无该字段 不进行编解码

		short msgLength = (short) (LongMessageFrameHolder.getPayloadLength(requestMessage.getMsgfmt().getAlphabet(),
				bodyBuffer.readUnsignedByte()) & 0xffff);

		byte[] contentbytes = new byte[msgLength];
		bodyBuffer.readBytes(contentbytes);
		requestMessage.setMsgContentBytes(contentbytes);
		requestMessage.setMsgLength((short) msgLength);
		// ly add
		// requestMessage.setMsgContent(requestMessage.getMsgContent());
		requestMessage.setMsgContent(msgContent);
		requestMessage.setReserve(bodyBuffer
				.readCharSequence(Cmpp20SubmitRequest.RESERVE.getLength(), GlobalConstance.defaultTransportCharset)
				.toString().trim());
		return requestMessage;
	}

	private void backup() throws UnsupportedEncodingException {
		byte[] body = null;
		// String test = JSONArray.toJSONString(body);
		// Object o = ProtoBufUtil.deserializer(body, MqEntity.class);
		// if (o instanceof CmppSubmitRequestMessage) {
		// LOGGER.info("yes!");
		// } else {
		// LOGGER.info("no!");
		// }
		//
		String message = new String(body, "UTF-8");
		// LOGGER.info(queueName + " received: " + envelope.getRoutingKey() + "
		// message: " + message);
		String appId;
		String msgId;
		// Object obj;
		CmppSubmitRequestMessage obj = null;
		// String s = "\"{hello {hey";
		// System.out.println("json to string x, " + JSON.toJSONString(s));
		// System.out.println(
		// "json to string 0, " + JSON.toJSONString(s).replaceAll("\\\\",
		// "123").replaceAll("\"\\{", "{"));
		// System.out.println("json to string 1, " +
		// JSON.toJSONString(message));
		// System.out.println("json to string 2, "
		// + JSON.toJSONString(message).replaceAll("\\\\",
		// "").replaceAll("\"\\{", "{").replaceAll("\\}\"", "}"));
		// MqEntity mqEntity = JSON.parseObject(
		// JSON.toJSONString(message).replaceAll("\\\\", "").replaceAll("\"\\{",
		// "{").replaceAll("\\}\"", "}"),
		// new TypeReference<MqEntity>() {
		// });
		MqEntity mqEntity = JSON.parseObject(message.replaceAll("\\\\", ""), MqEntity.class);
		LOGGER.info("mqEntity.getAppId() = " + mqEntity.getAppId());
		// if (mqEntity.getcObj() instanceof CmppSubmitRequestMessage) {
		// LOGGER.info("yes!");
		// } else {
		// LOGGER.info("no!");
		// }
		// try {
		// JSONObject jsonObject = JSONObject.parseObject(message);
		// appId = jsonObject.getString("appId");
		// msgId = jsonObject.getString("msgId");
		// obj = (CmppSubmitRequestMessage) jsonObject.get("obj");
		// JSON.parseObject(jsonResult, new
		// TypeReference<ResultBean<ModelPOBean>>() {
		// });
		// LOGGER.info("App用户appId={}的consumer接收到的消息体为{}", appId,
		// jsonObject.toJSONString());
		// } catch (Exception e) {
		// LOGGER.error("json格式转换失败，请检查参数......", e);
		// return;
		// }

		// if (obj instanceof CmppSubmitRequestMessage) {
		EndpointConnector<?> connector = EndpointManager.INS.getEndpointConnector("109002");
		while (true) {
			LOGGER.info("ready publish from consumer");
			ChannelFuture write = connector.asynwrite((CmppSubmitRequestMessage) obj);
			if (write != null) {
				break;
			} else {
				try {
					java.lang.Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		;
		// }

		// String sql = "from App where AppId = ?";
		// App app = ypDao.findSingle(sql, appId);
		// if (deleteOrderService.enableCancelOrder(app, language)
		// && deleteOrderService.cancelOrderSuccess(audioId, appId)) {
		// LOGGER.info("audioId: " + audioId + " cancel success, appId: " +
		// appId);
		// this.getChannel().basicAck(envelope.getDeliveryTag(), false);
		// return;
		// }
		// LOGGER.info("APP用户appId={}的音频audioId={}正在被消费", appId, audioId);
		// // 当没有空闲的转写路数时等待
		// while (!redisService.appIdCanConvert(appId, language)) {
		// LOGGER.info("当前APP用户appId={}没有多余的转写路数", appId);
		// try {
		// Thread.sleep(waitTime);
		// } catch (InterruptedException e) {
		// LOGGER.error(e.getMessage());
		// }
		// }
		// this.getChannel().basicAck(envelope.getDeliveryTag(), false);
		// AudioInfo audioInfo = ypDao.findSingle("from AudioInfo where Id = ?",
		// new Object[] {audioId});
		// audioInfo.setTranscriptStatus(AudioConsts.TRANSCRIPT_PROCCESSING);
		// audioInfo.setTransStartTime(new Date());
		// ypDao.save(audioInfo);
		// // 调用转写引擎
		// apiService.callEngineV2(appId, audioInfo, originalFile + ".wav");
		// LOGGER.info("app用户appId={}的音频audioId={}调用引擎转写成功", appId, audioId);
	}

	public static Object toObject(byte[] bytes) {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(bais);
			Object object = ois.readObject();
			return object;
		} catch (IOException ex) {
			throw new RuntimeException(ex.getMessage(), ex);
		} catch (ClassNotFoundException ex) {
			throw new RuntimeException(ex.getMessage(), ex);
		} finally {
			try {
				ois.close();
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		try {
			java.lang.Thread.sleep(2000);
		} catch (InterruptedException e1) {
			LOGGER.error(e1.getMessage(), e1);
		}
		LOGGER.error("v2 ShutdownSignalException by queueName = " + queueName + ", sig: " + sig);
		// Channel channel = null;
		// try {
		// channel = rabbitmqService.getChannel();
		// channel.confirmSelect();
		// channel.basicQos(1);
		// Consumer consumer = new AppConvertConsumer(rabbitmqService,
		// queueName, channel, redisService, apiService,
		// ypDao, waitTime, deleteOrderService);
		// channel.basicConsume(queueName, false, consumer);
		// } catch (Exception e) {
		// LOGGER.info(e.getMessage(), e);
		// }
		LOGGER.info("v2 new channel created by queueName = " + queueName);
	}

	@Override
	public void handleConsumeOk(String consumerTag) {
		LOGGER.info("transcodeConsumer handleConsumeOk: " + consumerTag);
	}

	@Override
	public void handleCancelOk(String consumerTag) {
		LOGGER.info("transcodeConsumer handleCancelOk: " + consumerTag);
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
		LOGGER.info("transcodeConsumer handleCancel: " + consumerTag);
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
		LOGGER.info("transcodeConsumer handleRecoverOk: " + consumerTag);
	}
}
