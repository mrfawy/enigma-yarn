package com.tito.easyyarn.ipc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Queue {
	private static final Log LOG = LogFactory.getLog(Queue.class);

	private Map<String, ReceiverAgent> p2pReceivers = new HashMap<>();
	private Map<String, List<ReceiverAgent>> topicReceivers = new HashMap<>();

	public boolean sendMessage(Message message) {
		if (message == null || message.getDestinationContext() == null) {
			LOG.error("Message is null, or no DestinationContext, aborting send");
			return false;
		}
		if (message.getDestinationContext().getDestinationType() == DestinationType.P2P) {
			String point = message.getDestinationContext().getDestinationId();
			if (!p2pReceivers.containsKey(point)) {
				LOG.error("Unknown point, no reciever subscribed for this id:" + point);
				return false;
			}
			return p2pReceivers.get(point).deliverMessage(message);
		}
		// Pub-sub
		else {
			String topic = message.getDestinationContext().getDestinationId();
			if (!topicReceivers.containsKey(topic)) {
				LOG.error("Unknown Topic, no recievers subscribed for this topic:" + topic);
				return false;
			}
			List<ReceiverAgent> recievers = topicReceivers.get(topic);
			boolean multiRecieveResult = false;
			for (ReceiverAgent receiverAgent : recievers) {
				boolean result = receiverAgent.deliverMessage(message);
				if (result) {
					multiRecieveResult = true;
				}
			}
			return multiRecieveResult;
		}

	}

}
