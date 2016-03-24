package com.tito.sampleapp.helloworld;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
		try {	
			Thread.sleep(10000);
			LOG.info("Hello world from HelloWorldTasklet!");
			MessagingServiceAgent.getInstance().broadCast(new TextMessage("Hello Messages! from: "+getId()));
			Thread.sleep(5000);
			return true;

		} catch (Exception ex) {
			LOG.error("Failed", ex);
			return false;
		}

	}

}
