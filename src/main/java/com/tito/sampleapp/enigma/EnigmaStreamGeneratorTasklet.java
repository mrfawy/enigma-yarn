package com.tito.sampleapp.enigma;

import java.io.IOException;

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
import com.tito.enigma.config.MachineConfig;
import com.tito.enigma.stream.StreamGenerator;

public class EnigmaStreamGeneratorTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(EnigmaStreamGeneratorTasklet.class);

	private String machineId;
	private String enigmaTempDir;
	private long length;

	StreamGenerator streamGenerator;

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

	private boolean generateStream() {
		LOG.info("Running generateStream");
		Configuration conf = new Configuration();
		FileSystem fs;
		FSDataInputStream fin = null;
		FSDataOutputStream fout = null;
		try {
			fs = FileSystem.get(conf);
			Path specFile = new Path(enigmaTempDir + Path.SEPARATOR + machineId + ".spec");
			if (!fs.exists(specFile)) {
				LOG.error("Spec file doesn't exist" + specFile);
				return false;
			}
			fin = fs.open(specFile);
			String confJson = fin.readUTF();
			MachineConfig machineConfig = new ObjectMapper().readValue(confJson, MachineConfig.class);

			Path keyFile = new Path(enigmaTempDir + Path.SEPARATOR + machineId + ".stream");
			if (fs.exists(keyFile)) {
				LOG.info("Replacing Stream file" + keyFile);
				fs.delete(keyFile, true);
			}
			fout = fs.create(keyFile);

			streamGenerator = new StreamGenerator(machineConfig);
			if (!streamGenerator.generateLength(length, fout)) {
				LOG.error("Failed to generate stream");
				return false;
			}
			LOG.info("Done generateStream for machine: " + machineId);
			return true;
		} catch (IOException e) {
			LOG.error("gemerateStream Error={}", e);
			return false;
		} finally {
			try {
				if (fin != null) {
					fin.close();
				}
				if (fout != null) {
					fout.close();
				}
				return true;

			} catch (IOException e) {

				LOG.error("error={}", e);
			}

		}

	}

}
