package com.i.server.rabbitmq.service;

import com.zx.sms.common.util.MsgId;

public class MqEntity {
	String appId;

	String channelId;
	// CmppSubmitRequestSelfDefinedMessage cmppSubmitRequestSelfDefinedMessage;
	// //
	// CmppSubmitRequestMessage cObject;

	String msgContent;

	public String getMsgContent() {
		return msgContent;
	}

	public void setMsgContent(String msgContent) {
		this.msgContent = msgContent;
	}

	MsgId msgId;

	Long ownSequenceId;

	String cmppMsgType;

	String cmppVersion;

	Object obj;

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	// public CmppSubmitRequestSelfDefinedMessage
	// getCmppSubmitRequestSelfDefinedMessage() {
	// return cmppSubmitRequestSelfDefinedMessage;
	// }
	//
	// public void setCmppSubmitRequestSelfDefinedMessage(
	// CmppSubmitRequestSelfDefinedMessage cmppSubmitRequestSelfDefinedMessage)
	// {
	// this.cmppSubmitRequestSelfDefinedMessage =
	// cmppSubmitRequestSelfDefinedMessage;
	// }
	//
	// public CmppSubmitRequestMessage getcObject() {
	// return cObject;
	// }
	//
	// public void setcObject(CmppSubmitRequestMessage cObject) {
	// this.cObject = cObject;
	// }

	public MsgId getMsgId() {
		return msgId;
	}

	public void setMsgId(MsgId msgId) {
		this.msgId = msgId;
	}

	public Long getOwnSequenceId() {
		return ownSequenceId;
	}

	public void setOwnSequenceId(Long ownSequenceId) {
		this.ownSequenceId = ownSequenceId;
	}

	public String getCmppMsgType() {
		return cmppMsgType;
	}

	public void setCmppMsgType(String cmppMsgType) {
		this.cmppMsgType = cmppMsgType;
	}

	public String getCmppVersion() {
		return cmppVersion;
	}

	public void setCmppVersion(String cmppVersion) {
		this.cmppVersion = cmppVersion;
	}

	public Object getObj() {
		return obj;
	}

	public void setObj(Object obj) {
		this.obj = obj;
	}

}
