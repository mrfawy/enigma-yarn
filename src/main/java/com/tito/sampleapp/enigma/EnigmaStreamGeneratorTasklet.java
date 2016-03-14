package com.tito.sampleapp.enigma;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import com.tito.easyyarn.task.Tasklet;
import com.tito.enigma.component.PlugBoard;
import com.tito.enigma.component.Reflector;
import com.tito.enigma.component.Rotor;
import com.tito.enigma.component.Util;
import com.tito.enigma.config.MachineConfig;
import com.tito.enigma.config.RotorConfig;

public class EnigmaStreamGeneratorTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(EnigmaStreamGeneratorTasklet.class);

	private static final int BUFFER_SIZE = 256 * 1000;

	private String machineId;
	private String enigmaTempDir;
	private long length;

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

		if (!commandLine.hasOption("enigmaTempDir")) {
			LOG.error("Missing enigmaTempDir");
			return false;
		} else {
			enigmaTempDir = commandLine.getOptionValue("enigmaTempDir");
		}

		if (!commandLine.hasOption("length")) {
			LOG.error("Missing length");
			return false;
		} else {
			length = Long.valueOf(commandLine.getOptionValue("length"));
		}

		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("machineId", true, "Engima Machine Id");
		opts.addOption("enigmaTempDir", true, "Directory to store key and streams");
		opts.addOption("length", true, "length of bytes to generate");

	}

	@Override
	public boolean start() {
		try {
			generateStream();
			return true;
		} catch (Exception e) {
			LOG.error("Failed To generate Key", e);
			return false;
		}

	}

	private void generateStream() {
		LOG.info("Running generateStream");
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path specFile = new Path(enigmaTempDir+Path.SEPARATOR + machineId + ".spec");

			if (!fs.exists(specFile)) {
				LOG.error("Spec file doesn't exist" + specFile);
				throw new RuntimeException("Spec file doesn't exist" + specFile);
			}
			FSDataInputStream fin = fs.open(specFile);
			String confJson = fin.readUTF();
			MachineConfig machineConfig = new ObjectMapper().readValue(confJson, MachineConfig.class);
			rotors = new ArrayList<>();
			for (RotorConfig rc : machineConfig.getRotorConfigs()) {
				rotors.add(new Rotor(rc));
			}
			reflector = new Reflector(machineConfig.getReflectorConfig());
			plugBoard = new PlugBoard(machineConfig.getPlugBoardConfig());
			generateLength(length);

			LOG.info("Done generateStream for machine: " + machineId);
		} catch (IOException e) {
			LOG.error("gemerateStream Error={}", e);
		}

	}

	private void generateLength(long n) {
		FSDataOutputStream fout = null;
		try {
			LOG.info("Generating length: " + n);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path keyFile = new Path(enigmaTempDir+Path.SEPARATOR + machineId + ".stream");
			if (fs.exists(keyFile)) {
				LOG.info("Replacing Stream file" + keyFile);
				fs.delete(keyFile, true);
			}
			fout = fs.create(keyFile);
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			buffer.clear();
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

				buffer.put(input);
				if (!buffer.hasRemaining()) {
					fout.write(buffer.array());
					buffer.clear();
				}

			}
			// copy the rest of n if exists
			if (buffer.position() != 0) {
				fout.write(buffer.array());
				buffer.clear();
			}
		} catch (Exception e) {
			LOG.error("Error={}", e);

		} finally {
			try {
				if (fout != null) {
					fout.close();
				}

			} catch (IOException e) {
				LOG.error("ex{}", e);
			}
		}

	}

}
