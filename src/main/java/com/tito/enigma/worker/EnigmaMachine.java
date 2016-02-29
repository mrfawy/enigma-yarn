package com.tito.enigma.worker;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.tito.enigma.Client;

public class EnigmaMachine {
	private static final Log LOG = LogFactory.getLog(Client.class);

	private BigDecimal length;

	// Command line options
	private Options opts;

	public EnigmaMachine() {
		opts = new Options();
		opts.addOption("length", true, "The length of the generate mapad byte stream");
		opts.addOption("help", false, "Print usage");
	}

	/**
	 * Parse command line options
	 * 
	 * @param args
	 *            Parsed command line options
	 * @return Whether the init was successful to run the client
	 * @throws ParseException
	 */
	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("length")) {
			length = new BigDecimal(cliParser.getOptionValue("length"));
			LOG.info("Generate stream of length:" + length);
		}
		if (length.compareTo(new BigDecimal(0)) < 0) {
			throw new IllegalArgumentException("Byte stream length cannot be less than or equal zero");
		}

		return true;
	}

	private void printUsage() {
		new HelpFormatter().printHelp("EnigmaMachine", opts);
	}

	public boolean run() throws IOException, YarnException {
		LOG.info("Running EnigmaMachine");
		return true;
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			EnigmaMachine EnigmaMachine = new EnigmaMachine();
			LOG.info("Initializing EnigmaMachine");
			try {
				boolean doRun = EnigmaMachine.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				EnigmaMachine.printUsage();
				System.exit(-1);
			}
			result = EnigmaMachine.run();
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
