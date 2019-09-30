package com.zx.sms.codec.cmpp.msg;

public class CmppDeliverRequestSelfDefinedMessage {
	private Header header;
	private byte[] bodyBuffer;

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public byte[] getBodyBuffer() {
		return bodyBuffer;
	}

	public void setBodyBuffer(byte[] bodyBuffer) {
		this.bodyBuffer = bodyBuffer;
	}
}
