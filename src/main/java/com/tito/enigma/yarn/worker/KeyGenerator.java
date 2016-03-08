package com.tito.enigma.yarn.worker;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.machine.config.ConfigGenerator;

public class KeyGenerator {
	private static final Log LOG = LogFactory.getLog(KeyGenerator.class);

	private Options opts;
	private String keyDir;
	private String id;
	private int minRotorCount;
	private int maxRotorCount;

	public KeyGenerator() {
		opts = new Options();
		opts.addOption("id", true, "configuration Id");		
		opts.addOption("keyDir", true, "");		
		opts.addOption("minRotorCount", true, "min Rotor Count for a machine");
		opts.addOption("maxRotorCount", true, "max Rotor Count");		
		opts.addOption("help", false, "Print usage");
	}

	public boolean init(String[] args) throws ParseException {

		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}

		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}

		if (cliParser.hasOption("id")) {
			id = cliParser.getOptionValue("id");
		} else {
			throw new IllegalArgumentException(" KeyGenerator: Missing Id");
		}
		if (cliParser.hasOption("keyDir")) {
			keyDir = cliParser.getOptionValue("keyDir");
		} else {
			throw new IllegalArgumentException(" KeyGenerator: Missing KeyDir");
		}
		minRotorCount = Integer.parseInt(cliParser.getOptionValue("minRotorCount", "10"));
		maxRotorCount = Integer.parseInt(cliParser.getOptionValue("maxRotorCount", "10"));

		return true;
	}

	private void printUsage() {
		new HelpFormatter().printHelp("KeyGenerator", opts);
	}

	public boolean run() throws IOException, YarnException {
		LOG.info("Running KeyGenerator");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path keyFile = Path.mergePaths(new Path(keyDir), new Path(id + ".key"));
		if (fs.exists(keyFile)) {
			LOG.info("Replacing Key file" + keyFile);
			fs.delete(keyFile, true);
		}
		FSDataOutputStream fin = fs.create(keyFile);
		ConfigGenerator gen = new ConfigGenerator();
		String confJson = new ObjectMapper()
				.writeValueAsString(gen.generateConfiguration(minRotorCount, maxRotorCount));
		fin.writeUTF(confJson);
		fin.close();

		LOG.info("Done writing Key" + keyFile);
		return true;

	}

	public static void main(String[] args) {
		System.out.println("Inside Key Generator main method");
		boolean result = false;
		try {
			KeyGenerator keyGenerator = new KeyGenerator();
			LOG.info("Initializing EnigmaMachine");
			try {
				boolean doRun = keyGenerator.init(args);
				if (!doRun) {
					System.exit(0);
				}
			} catch (IllegalArgumentException e) {
				System.err.println(e.getLocalizedMessage());
				keyGenerator.printUsage();
				System.exit(-1);
			}
			result = keyGenerator.run();
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
