package com.tito.easyyarn.service.messaging;

import java.net.ServerSocket;
import java.util.Hashtable;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;
import org.jgroups.util.Util;

public class MessagingServiceAgent implements ChannelListener {
	private static final Log LOG = LogFactory.getLog(MessagingServiceAgent.class);

	// default topic to connect containers;
	private static final String EASY_YARN_TOPIC = "EASY_YARN_CORE";

	private Map<String, JChannel> topics = new Hashtable<>();
	private static MessagingServiceAgent me;
	private String id;
	private Receiver reciever;
	private int freePort=-1;

	private MessagingServiceAgent(String id, Receiver reciever) {
		this.id = id;
		this.reciever = reciever;
		init();
	}

	private void init() {
		if (!subscribe(EASY_YARN_TOPIC)) {
			throw new RuntimeException("Failed to subscribe to :" + EASY_YARN_TOPIC);
		}

		// todo : use zookeeper to save ID->address
	}

	public static void initInstance(String id, Receiver reciever) {
		if (me == null) {
			me = new MessagingServiceAgent(id, reciever);
		} else {
			throw new RuntimeException("Instance already initialized , call getInstance instead");
		}

	}

	public static MessagingServiceAgent getInstance() {
		if (me == null) {
			throw new RuntimeException("Instance not initialized , call initInstance first");
		}
		return me;
	}

	public boolean send(String targetId, MessageBody msg) {

		try {
			Address address = resolveAddress(targetId);
			topics.get(EASY_YARN_TOPIC).send(address, msg);
			return true;

		} catch (Exception e) {
			LOG.error("err={}", e);
			return false;
		}
	}

	private Address resolveAddress(String id) {
		return null;
	}

	public boolean subscribe(String topic) {
		if (!topics.containsKey(topic)) {
			JChannel channel;
			try {
				channel = new JChannel(this.getClass().getResource("/jgroups-channel.xml"));
				channel.getProtocolStack().getTransport().setBindPort(getFreePort());
				// channel.setName(topic + "_"+id);
				channel.setDiscardOwnMessages(true);
				channel.setReceiver(this.reciever);
				channel.addChannelListener(this);
				channel.connect(topic);
				topics.put(topic, channel);
				return true;
			} catch (Exception e) {
				LOG.error("err={}", e);
				return false;
			}
		} else {
			LOG.error("error={}", new RuntimeException("Already subscribed to topic: " + topic));
			return false;
		}

	}

	public boolean unSubscribe(String topic) {
		if (!topics.containsKey(topic)) {
			LOG.error("error={}", new RuntimeException("Topic not found: " + topic));
			return false;
		}
		topics.get(topic).close();
		topics.remove(topic);
		return true;

	}

	public boolean publish(String topic, MessageBody msgBody) {
		Message msg = new Message(null, null, msgBody);
		try {
			topics.get(topic).send(msg);
			return true;
		} catch (Exception e) {
			LOG.error("publish error={}", e);
			return false;
		}

	}

	public boolean broadCast(MessageBody msgBody) {
		try {
			Message msg = new Message(null, null, Util.objectToByteBuffer(msgBody));
			topics.get(EASY_YARN_TOPIC).send(msg);
			return true;
		} catch (Exception e) {
			LOG.error("publish error={}", e);
			return false;
		}

	}
	public int getFreePort() {
		if (freePort == -1) {
			try {
				ServerSocket socket = new ServerSocket(0);
				freePort = socket.getLocalPort();				
				socket.close();
			} catch (Exception e) {
				LOG.error("Can't find open port");
				return -1;
			}

		}
		return freePort;
	}


	@Override
	protected void finalize() throws Throwable {
		for (String topic : topics.keySet()) {
			if (!topics.get(topic).isClosed()) {
				topics.get(topic).close();
			}
		}
	}

	@Override
	public void channelClosed(Channel arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelConnected(Channel arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelDisconnected(Channel arg0) {
		// TODO Auto-generated method stub

	}

}
