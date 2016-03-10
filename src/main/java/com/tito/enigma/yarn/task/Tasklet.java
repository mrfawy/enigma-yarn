package com.tito.enigma.yarn.task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.enigma.config.ExtendedGnuParser;

public abstract class Tasklet implements TaskletIF {
	private static final Log LOG = LogFactory.getLog(Tasklet.class);
	
	private Options options;
	
	private void printUsage() {
		new HelpFormatter().printHelp("Tasklet",getOptions());
	}

	public Options setupOptionsAll() {
		options = getMainClassOption();		
		setupOptions(options);
		return options;

	}
	
	public static Options getMainClassOption(){
		Options ops = new Options();
		ops.addOption("TaskletClass", true, "Tasklet Class");
		return ops;
	}
	
	public boolean initAll(CommandLine cliParser) {	
		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}
		return init(cliParser);
	}

	
	public static void main(String[] args) {
		System.out.println("Inside Tasklet main method");
		boolean result = false;
		try {				
			Options ops = getMainClassOption();			
			CommandLine cliParser1 = new ExtendedGnuParser(true).parse(ops, args);
			if (!cliParser1.hasOption("TaskletClass")) {
				throw new RuntimeException("TaskletClass is not specified failed to load Tasklet");
			}

			Tasklet tasklet=(Tasklet) Class.forName(cliParser1.getOptionValue("TaskletClass"))
					.newInstance();
			
			LOG.info("Initializing Tasklet");
			try {
				tasklet.setupOptionsAll();
				CommandLine cliParser = new GnuParser().parse(tasklet.getOptions(), args);				
				boolean doRun = tasklet.init(cliParser);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());				
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

	public Options getOptions() {
		return options;
	}

	public void setOptions(Options options) {
		this.options = options;
	}
	

}
