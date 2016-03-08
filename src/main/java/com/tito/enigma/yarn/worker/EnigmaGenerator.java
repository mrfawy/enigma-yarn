package com.tito.enigma.yarn.worker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.machine.PlugBoard;
import com.tito.enigma.machine.Reflector;
import com.tito.enigma.machine.Rotor;
import com.tito.enigma.machine.Util;
import com.tito.enigma.machine.config.ConfigGenerator;
import com.tito.enigma.machine.config.MachineConfig;
import com.tito.enigma.machine.config.RotorConfig;
import com.tito.enigma.queue.Queue;

public class EnigmaGenerator {
	private static final Log LOG = LogFactory.getLog(EnigmaGenerator.class);
	private Options opts;
	private String id;
	private String keyDir;
	private long length;

	List<Rotor> rotors;
	Reflector reflector;
	PlugBoard plugBoard;

	private void printUsage() {
		new HelpFormatter().printHelp("EnigmaGenerator", opts);
	}

	public boolean run() throws IOException, YarnException {
		LOG.info("Running EnigmaGenerator");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path keyFile = Path.mergePaths(new Path(keyDir), new Path(id + ".key"));

		if (!fs.exists(keyFile)) {
			LOG.error("File not exists Key file" + keyFile);
			throw new RuntimeException("Key file doesn't exist" + keyFile);
		}
		FSDataInputStream fin = fs.open(keyFile);
		ConfigGenerator gen = new ConfigGenerator();
		String confJson = fin.readUTF();
		MachineConfig machineConfig = new ObjectMapper().readValue(confJson, MachineConfig.class);
		rotors = new ArrayList<>();
		for (RotorConfig rc : machineConfig.getRotorConfigs()) {
			rotors.add(new Rotor(rc));
		}
		reflector = new Reflector(machineConfig.getReflectorConfig());
		plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
		generateLength(length);

		LOG.info("Done EnigmaGenerator: " + id);
		return true;
	}

	public void generateLength(long n) {
		LOG.info("Generating length: " + n);
		byte[] buffer = new byte[256 * 1000];
		int bufferOffset = 0;
		int bufferCount = 0;
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

			if (bufferOffset + input.length <= buffer.length) {
				System.arraycopy(input, 0, buffer, bufferOffset, input.length);
				bufferOffset += input.length;

			} else {// if buffer is full
				Queue.geteInstance().put(id, String.valueOf(bufferCount), buffer);
				bufferOffset = 0;
				bufferCount++;
				System.arraycopy(input, 0, buffer, bufferOffset, input.length);
				bufferOffset += input.length;
			}

		}
		// copy the rest of n if exists
		if (bufferOffset != 0) {
			Queue.geteInstance().put(id, String.valueOf(bufferCount), buffer);
		}

	}

	public EnigmaGenerator() {
		opts = new Options();
		opts.addOption("id", true, "configuration Id");
		opts.addOption("keyDir", true, "");
		opts.addOption("length", true, "The length of the generated map byte stream");
		opts.addOption("help", false, "Print usage");

	}

	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to EnigmaGenerator to intialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("id")) {
			id = cliParser.getOptionValue("id");
			LOG.info("Generate stream of ID:" + id);
		} else {
			throw new IllegalArgumentException("EnigmaGenerator:Missing ID");
		}
		if (cliParser.hasOption("keyDir")) {
			keyDir = cliParser.getOptionValue("keyDir");
		} else {
			throw new IllegalArgumentException("EnigmaGenerator:Missing keyDir");
		}
		if (cliParser.hasOption("length")) {
			length = Long.parseLong(cliParser.getOptionValue("length"));
		} else {
			throw new IllegalArgumentException("EnigmaGenerator:Missing length");
		}

		return true;
	}

	public static void main(String[] args) {
		System.out.println("Inside EnigmaGenerator main method");
		boolean result = false;
		try {
			EnigmaGenerator enigmaGenerator = new EnigmaGenerator();
			LOG.info("Initializing EnigmaGenerator");
			try {
				boolean doRun = enigmaGenerator.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				enigmaGenerator.printUsage();
				System.exit(-1);
			}
			result = enigmaGenerator.run();
		} catch (Throwable t) {
			LOG.fatal("Error running EnigmaGenerator", t);
			System.exit(1);
		}
		if (result) {
			LOG.info("EnigmaGenerator completed successfully");
			System.exit(0);
		}
		LOG.error("EnigmaGenerator failed to complete successfully");
		System.exit(2);
	}
}
