package com.tito.enigma.yarn.app.enigma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.machine.PlugBoard;
import com.tito.enigma.machine.Reflector;
import com.tito.enigma.machine.Rotor;
import com.tito.enigma.machine.Util;
import com.tito.enigma.machine.config.ConfigGenerator;
import com.tito.enigma.machine.config.MachineConfig;
import com.tito.enigma.machine.config.RotorConfig;
import com.tito.enigma.queue.Queue;
import com.tito.enigma.yarn.task.Tasklet;

public class EnigmaGeneratorTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(EnigmaGeneratorTasklet.class);

	private String machineId;
	private String keyDir;
	private String tempStreamDir;
	private long length;
	int minRotorCount;
	int maxRotorCount;
	
	List<Rotor> rotors;
	Reflector reflector;
	PlugBoard plugBoard;

	@Override
	public boolean init(CommandLine commandLine) {

		if (!commandLine.hasOption("machineId")) {
			LOG.error("Missing machineId");
			return false;
		} else {
			machineId = commandLine.getOptionValue("machineId");
		}

		if (!commandLine.hasOption("keyDir")) {
			LOG.error("Missing keyDir");
			return false;
		} else {
			keyDir = commandLine.getOptionValue("keyDir");
		}

		if (!commandLine.hasOption("tempStreamDir")) {
			LOG.error("Missing tempStreamDir");
			return false;
		} else {
			tempStreamDir = commandLine.getOptionValue("tempStreamDir");
		}
		if (!commandLine.hasOption("length")) {
			LOG.error("Missing length");
			return false;
		} else {
			length = Long.valueOf(commandLine.getOptionValue("length"));
		}

		if (!commandLine.hasOption("minRotorCount")) {
			minRotorCount = 1;
		} else {
			minRotorCount = Integer.valueOf(commandLine.getOptionValue("minRotorCount"));
		}

		if (!commandLine.hasOption("maxRotorCount")) {
			maxRotorCount = 5;
		} else {
			maxRotorCount = Integer.valueOf(commandLine.getOptionValue("maxRotorCount"));
		}
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("machineId", true, "Engima Machine Id");
		opts.addOption("keyDir", true, "Directory to store key");
		opts.addOption("length", true, "length of bytes to generate");
		opts.addOption("tempStreamDir", true, "Directory to write internal machine streams");
		opts.addOption("minRotorCount", true, "min Rotor Count for a machine");
		opts.addOption("maxRotorCount", true, "max Rotor Count");

	}

	@Override
	public boolean start() {
		try {
			generateKey();
		} catch (IOException e) {
			LOG.error("Failed To generate Key",e);
			return false;
		}
		return false;
	}

	private void generateKey() throws IOException {
		LOG.info("Running KeyGenerator");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path keyFile = Path.mergePaths(new Path(keyDir), new Path(machineId + ".key"));
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
	}

	private void generateStream() {
		LOG.info("Running generateStream");
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path keyFile = Path.mergePaths(new Path(keyDir), new Path(machineId+ ".key"));

			if (!fs.exists(keyFile)) {
				LOG.error("File not exists Key file" + keyFile);
				throw new RuntimeException("Key file doesn't exist" + keyFile);
			}
			FSDataInputStream fin = fs.open(keyFile);
			String confJson = fin.readUTF();
			MachineConfig machineConfig = new ObjectMapper().readValue(confJson, MachineConfig.class);
			rotors = new ArrayList<>();
			for (RotorConfig rc : machineConfig.getRotorConfigs()) {
				rotors.add(new Rotor(rc));
			}
			reflector = new Reflector(machineConfig.getReflectorConfig());
			plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
			generateLength(length);

			LOG.info("Done EnigmaGenerator: " + machineId);
		} catch (IOException e) {			
			LOG.error("gemerateStream Error={}",e);
		}
		
	}
	private void generateLength(long n) {
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
				Queue.geteInstance().put(machineId, String.valueOf(bufferCount), buffer);
				bufferOffset = 0;
				bufferCount++;
				System.arraycopy(input, 0, buffer, bufferOffset, input.length);
				bufferOffset += input.length;
			}

		}
		// copy the rest of n if exists
		if (bufferOffset != 0) {
			Queue.geteInstance().put(machineId, String.valueOf(bufferCount), buffer);
		}

	}
	 

}
