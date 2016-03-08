package com.tito.enigma.machine;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.tito.enigma.machine.config.MachineConfig;
import com.tito.enigma.machine.config.RotorConfig;
import com.tito.enigma.yarn.client.Client;

public class EnigmaMachine {
	private static final Log LOG = LogFactory.getLog(Client.class);

	private BigDecimal length;

	// Command line options
	private Options opts;

	private MachineConfig machineConfig;

	List<Rotor> rotors;
	Reflector reflector;
	PlugBoard plugBoard;

	public EnigmaMachine() {

	}

	public EnigmaMachine(MachineConfig machineConfig) {
		opts = new Options();
		opts.addOption("length", true, "The length of the generate mapad byte stream");
		opts.addOption("input", true, "file to read");
		opts.addOption("output", true, "file to write");
		opts.addOption("help", false, "Print usage");
		this.machineConfig = machineConfig;
		rotors = new ArrayList<>();
		for (RotorConfig rc : machineConfig.getRotorConfigs()) {
			rotors.add(new Rotor(rc));
		}
		reflector = new Reflector(machineConfig.getReflectorConfig());
		plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
	}

	public void generateLength(long n) {

		byte[] input = Util.getArray(256);
		for (long i = 0; i < n; i++) {
			input = plugBoard.signalIn(input);
			for (Rotor r : rotors) {
				input = r.signalIn(input);
				r.rotate();
			}
			input = reflector.signalIn(input);
			for (int j = rotors.size() - 1; j >= 0; j--) {
				input = rotors.get(j).reverseSignalIn(input);
			}
			input = plugBoard.reverseSignalIn(input);

			// process stepping/rotating
			boolean rotateFlag;
			int rotorIndex = 0;
			do {
				rotateFlag = rotors.get(rotorIndex++).rotate();
			} while (rotateFlag == true);

		}
		System.out.println(input);
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
		/*
		 * if (args.length == 0) { throw new IllegalArgumentException(
		 * "No args specified for client to initialize"); }
		 */

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("length")) {
			length = new BigDecimal(cliParser.getOptionValue("length"));
			if (length.compareTo(new BigDecimal(0)) < 0) {
				throw new IllegalArgumentException("Byte stream length cannot be less than or equal zero");
			}
			LOG.info("Generate stream of length:" + length);
		}
		if (cliParser.hasOption("input")) {
			// input = cliParser.getOptionValue("input");
		}
		if (cliParser.hasOption("output")) {
			// output = cliParser.getOptionValue("output");
		}

		return true;
	}

	private void printUsage() {
		new HelpFormatter().printHelp("EnigmaMachine", opts);
	}

	public boolean run() throws IOException, YarnException {
		LOG.info("Running EnigmaMachine");

		System.out.println("Hello from Inside Enigma worker");
		return true;
	}

	public static void main(String[] args) {
		System.out.println("Inside Engima Machine main method");
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
