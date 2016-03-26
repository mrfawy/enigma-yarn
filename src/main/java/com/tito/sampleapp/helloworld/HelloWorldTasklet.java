package com.tito.sampleapp.helloworld;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.tito.easyyarn.hazel.GridServiceAgent;
import com.tito.easyyarn.service.messaging.MessagingServiceAgent;
import com.tito.easyyarn.task.Tasklet;

public class HelloWorldTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(HelloWorldTasklet.class);

	@Override
	public boolean init(CommandLine commandLine) {
		return true;
	}

	@Override
	public void setupOptions(Options opts) {

	}

	@Override
	public boolean start() {
		// callHazel();
		return callMessages();

	}

	private boolean callHazel() {
		try {
			Thread.sleep(2000 + 1000 * new Random().nextInt(10));
			Map<Integer, String> customers = GridServiceAgent.getInstance().getHazelInstance().getMap("customers");

			String ID = UUID.randomUUID().toString();
			LOG.info("ID : " + ID);
			customers.put(1, "Joe_" + ID);
			customers.put(2, "Ali_" + ID);
			customers.put(3, "Avi_" + ID);

			Thread.sleep(2000 + 1000 * new Random().nextInt(10));

			HazelcastInstance client = GridServiceAgent.getInstance().getHazelInstance();
			IMap map = client.getMap("customers");
			LOG.info("Map Size:" + map.size());
			return true;

		} catch (Exception ex) {
			LOG.error("Failed", ex);
			return false;
		}

	}

	private boolean callMessages() {
		try {
			Thread.sleep(2000 + 1000 * new Random().nextInt(10));
			LOG.info("Hello world from HelloWorldTasklet!");
			MessagingServiceAgent.getInstance().broadCast(new TextMessage("Hello Messages! from: " + UUID.randomUUID().toString()));
			Thread.sleep(2000 + 1000 * new Random().nextInt(5));
			return true;

		} catch (Exception ex) {
			LOG.error("Failed", ex);
			return false;
		}
	}

}
