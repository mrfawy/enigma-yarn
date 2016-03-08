package com.tito.enigma.yarn.worker;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class EnigmaCombiner {
	private static final Log LOG = LogFactory.getLog(EnigmaCombiner.class);
	
	private String[] idList;
	private String input;
	private String output;
	

	private Options opts;

	public EnigmaCombiner() {
		opts = new Options();
		opts.addOption("idList", true, "Id list of machines , comma separated");
		opts.addOption("input", true, "file to read");
		opts.addOption("output", true, "file to write");
		opts.addOption("help", false, "Print usage");
	}

	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);
		/*
		 * if (args.length == 0) { throw new IllegalArgumentException(
		 * "No args specified for client to initialize"); }
		 */

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("idList")) {
			idList = cliParser.getOptionValue("idList").split(",",-1);
			if (idList==null||idList.length==0) {
				throw new IllegalArgumentException("Enigma IDs is less than or equal zero");
			}
			LOG.info("Combinging results from:" + Arrays.toString(idList));
		}
		else{
			throw new IllegalArgumentException("Missing idList");
		}
		if (cliParser.hasOption("input")) {
			input = cliParser.getOptionValue("input");
		}
		else{
			throw new IllegalArgumentException("Missing input");
		}
		if (cliParser.hasOption("output")) {
			output = cliParser.getOptionValue("output");
		}
		else{
			throw new IllegalArgumentException("Missing output");
		}

		return true;
	}
	
	public boolean run() throws IOException, YarnException {
		LOG.info("Running Engima Combiner");

		System.out.println("Hello from Inside Enigma worker");
		return true;
	}

	private void printUsage() {
		new HelpFormatter().printHelp("Enigma Combiner", opts);
	}

	public static void main(String[] args) {
		System.out.println("Inside Engima Combiner main method");
		boolean result = false;
		try {
			EnigmaCombiner combiner = new EnigmaCombiner();
			LOG.info("Initializing EnigmaMachine");
			try {
				boolean doRun = combiner.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				combiner.printUsage();
				System.exit(-1);
			}
			result = combiner.run();
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
