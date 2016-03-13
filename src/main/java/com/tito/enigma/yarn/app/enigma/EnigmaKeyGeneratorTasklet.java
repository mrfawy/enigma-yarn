package com.tito.enigma.yarn.app.enigma;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.machine.config.ConfigGenerator;
import com.tito.enigma.yarn.task.Tasklet;

public class EnigmaKeyGeneratorTasklet extends Tasklet {

	private static final Log LOG = LogFactory.getLog(EnigmaKeyGeneratorTasklet.class);

	private String machineId;
	private String enigmaTempDir;
	int minRotorCount;
	int maxRotorCount;

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

		minRotorCount = Integer.valueOf(commandLine.getOptionValue("minRotorCount", "1"));

		maxRotorCount = Integer.valueOf(commandLine.getOptionValue("maxRotorCount", "5"));

		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("machineId", true, "Engima Machine Id");
		opts.addOption("enigmaTempDir", true, "Directory to store key and streams");
		opts.addOption("minRotorCount", true, "min Rotor Count for a machine");
		opts.addOption("maxRotorCount", true, "max Rotor Count");

	}

	@Override
	public boolean start() {
		try {
			generateKey();
			return true;
		} catch (Exception e) {
			LOG.error("Failed To generate Key", e);
			return false;
		}

	}

	private void generateKey() {
		LOG.info("Running KeyGenerator");
		FSDataOutputStream fout = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path keyFile = Path.mergePaths(new Path(enigmaTempDir),
					new Path(Path.SEPARATOR + "key" + Path.SEPARATOR + machineId + ".key"));
			if (fs.exists(keyFile)) {
				LOG.info("Replacing Key file" + keyFile);
				fs.delete(keyFile, true);
			}
			fout = fs.create(keyFile);
			ConfigGenerator gen = new ConfigGenerator();
			String confJson = new ObjectMapper()
					.writeValueAsString(gen.generateConfiguration(minRotorCount, maxRotorCount));
			fout.writeUTF(confJson);

		} catch (Exception ex) {
			LOG.error("Error={}", ex);
		} finally {
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
	}

}
