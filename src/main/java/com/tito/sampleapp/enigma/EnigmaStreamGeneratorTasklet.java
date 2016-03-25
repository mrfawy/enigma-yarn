package com.tito.sampleapp.enigma;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.tito.easyyarn.task.Tasklet;
import com.tito.enigma.avro.EnigmaKey;
import com.tito.enigma.avro.MachineConfig;
import com.tito.enigma.component.EnigmaKeyUtil;
import com.tito.enigma.stream.StreamGenerator;

public class EnigmaStreamGeneratorTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(EnigmaStreamGeneratorTasklet.class);

	private String keyPath;
	private String machineId;
	private String enigmaTempDir;
	private long length;

	StreamGenerator streamGenerator;

	@Override
	public boolean init(CommandLine commandLine) {
		
		if (!commandLine.hasOption("keyPath")) {
			LOG.error("Missing keyPath");
			return false;
		} else {
			keyPath = commandLine.getOptionValue("keyPath");
		}

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
		opts.addOption("keyPath", true, "Path to EnigmaKey.key file");
		opts.addOption("machineId", true, "Engima Machine Id");
		opts.addOption("enigmaTempDir", true, "Directory to store key and streams");
		opts.addOption("length", true, "length of bytes to generate");

	}

	@Override
	public boolean start() {
		try {
			return generateStream();		
		} catch (Exception e) {
			LOG.error("Failed To generate Stream", e);
			return false;
		}

	}

	private boolean generateStream() {
		LOG.info("Running generateStream");
		
		Configuration conf = new Configuration();
		FileSystem fs;	
		FSDataOutputStream fout = null;
		try {
			fs = FileSystem.get(conf);	
			EnigmaKey enigmaKey=EnigmaKeyUtil.loadKey(keyPath);			
			MachineConfig machineConfig = enigmaKey.getMachineConfig().get(machineId);
			if(machineConfig==null){			
				LOG.error("MachineConfig is null, aborting");
				return false;
			}

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
		} catch (Exception e) {
			LOG.error("gemerateStream Error={}", e);
			return false;
		} finally {
			try {			
				if (fout != null) {
					fout.close();
				}				

			} catch (IOException e) {

				LOG.error("error={}", e);
			}

		}

	}

}
