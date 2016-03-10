package com.tito.enigma.yarn.task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Tasklet implements TaskletIF {
	private static final Log LOG = LogFactory.getLog(Tasklet.class);
	
	private Options options;
	
	private static void printUsage() {
		new HelpFormatter().printHelp("Tasklet", getInstance().getOptions());
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();
		return opts;

	}

	@Override
	public boolean init(CommandLine cliParser) {		
		return true;
	}
	public static void main(String[] args) {
		System.out.println("Inside Tasklet main method");
		boolean result = false;
		try {				
			Options ops = new Options();
			ops.addOption("TaskletClass", true, "Tasklet Class");
			CommandLine cliParser1 = new GnuParser().parse(ops, args);
			if (!cliParser1.hasOption("TaskletClass")) {
				throw new RuntimeException("TaskletClass is not specified failed to load Tasklet");
			}

			TaskletIF tasklet=(TaskletIF) Class.forName(cliParser1.getOptionValue("TaskletClass"))
					.newInstance();
			
			LOG.info("Initializing Tasklet");
			try {
				CommandLine cliParser = new GnuParser().parse(tasklet.getOptions(), args);
				boolean doRun = tasklet.init(cliParser);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				printUsage();
				System.exit(-1);
			}
			result = tasklet.start();
		} catch (Throwable t) {
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
	

}
