package com.tito.sampleapp.helloworld;

import com.tito.easyyarn.service.messaging.MessageBody;

public class TextMessage extends MessageBody {

	private String msg;

	public TextMessage(String msg) {
		super();
		this.msg = msg;
	}

	@Override
	public String toString() {
		return "TextMessage [msg=" + msg + "]";
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

}
