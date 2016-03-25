package com.tito.easyyarn.task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Message;

import com.tito.easyyarn.hazel.GridServiceAgent;
import com.tito.easyyarn.service.messaging.MessagingServiceAgent;
import com.tito.easyyarn.util.ExtendedGnuParser;

public abstract class Tasklet {
	private static final Log LOG = LogFactory.getLog(Tasklet.class);

	private Options options;
	private String id;
	private TaskletReciever taskletReciever;

	public abstract void setupOptions(Options options);

	public abstract boolean init(CommandLine cliParser);

	public abstract boolean start();

	public Tasklet() {
		taskletReciever = new TaskletReciever(this);
	}

	private void printUsage() {
		new HelpFormatter().printHelp("Tasklet", getOptions());
	}
	
	public void recieveMessage(Message msg){
		LOG.info("Message Recieved");
		LOG.info(msg.toString());
	}
	public Options setupOptionsAll() {
		options = getMainClassOption();
		options.addOption("id", true, "Tasklet Id , a unique UUID will be used if not provided");
		setupOptions(options);
		return options;

	}

	public static Options getMainClassOption() {
		Options ops = new Options();
		ops.addOption("TaskletClass", true, "Tasklet Class");
		return ops;
	}

	public boolean initAllOptions(CommandLine cliParser) {
		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}
		this.id = cliParser.getOptionValue("id", java.util.UUID.randomUUID().toString());
		return init(cliParser);
	}

	public boolean startServices() {
		MessagingServiceAgent.initInstance(id, taskletReciever);
		GridServiceAgent.initInstance(id);
		return true;
	}

	public static void main(String[] args) {
		System.out.println("Inside Tasklet main method");
		boolean result = false;
		try {
			Options ops = getMainClassOption();
			CommandLine cliParser = new ExtendedGnuParser(true).parse(ops, args);
			if (!cliParser.hasOption("TaskletClass")) {
				throw new RuntimeException("TaskletClass is not specified failed to load Tasklet");
			}

			Tasklet tasklet = (Tasklet) Class.forName(cliParser.getOptionValue("TaskletClass")).newInstance();

			LOG.info("Initializing Tasklet");

			ops = tasklet.setupOptionsAll();
			// reparse args to assign options
			cliParser = new ExtendedGnuParser(true).parse(ops, args);
			boolean doRun = tasklet.init(cliParser);
			if (!doRun) {
				System.exit(1);
			}
			tasklet.startServices();
			result = tasklet.start();
		} catch (Exception t) {
			LOG.fatal("Error running CLient", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("Application completed successfully");
			System.exit(0);
		}
		LOG.error("Application failed to complete successfully");
		System.exit(2);
	}

	public Options getOptions() {
		return options;
	}

	public void setOptions(Options options) {
		this.options = options;
	}

	public String getId() {
		return id;
	}

}
