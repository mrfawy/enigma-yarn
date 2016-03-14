package com.tito.sampleapp.enigma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.tito.enigma.component.Util;
import com.tito.enigma.config.EnigmaKey;

public class EnigmaCombinerTasklet extends Tasklet {

	private static final Log LOG = LogFactory.getLog(EnigmaCombinerTasklet.class);

	private static final int INPUT_BUFFER_SIZE = 1024 * 100;
	private static final int STREAM_BUFFER_SIZE = 256 * INPUT_BUFFER_SIZE;

	private String enigmaTempDir;
	private String keyPath;
	private String inputPath;
	private String outputPath;

	List<String> machineSequence;
	List<FSDataInputStream> machineStreamList;

	@Override
	public boolean init(CommandLine commandLine) {
		if (!commandLine.hasOption("keyPath")) {
			LOG.error("Missing keyPath");
			return false;
		}
		keyPath = commandLine.getOptionValue("keyPath");

		if (!commandLine.hasOption("enigmaTempDir")) {
			LOG.error("Missing enigmaTempDir");
			return false;
		}
		enigmaTempDir = commandLine.getOptionValue("enigmaTempDir");

		if (!commandLine.hasOption("inputPath")) {
			LOG.error("Missing inputPath");
			return false;
		}
		inputPath = commandLine.getOptionValue("inputPath");

		if (!commandLine.hasOption("outputPath")) {
			LOG.error("Missing outputPath");
			return false;
		}
		outputPath = commandLine.getOptionValue("outputPath");

		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("keyPath", true, "EngimaKey.key file path");
		opts.addOption("enigmaTempDir", true, "Enigma temp directory , looking for /key/EnigmaKey & /stream/");
		opts.addOption("inputPath", true, "input file to combine path");
		opts.addOption("outputPath", true, "output file to generate");

	}

	@Override
	public boolean start() {
		machineSequence = getMachineSequence();
		if (machineSequence == null) {
			return false;
		}
		machineStreamList = getMachineStreams(machineSequence);
		if (machineStreamList == null) {
			return false;
		}

		try {
			processStreams();
		} catch (IOException e) {
			LOG.error("FAILED TO PROCESS STREAMS", e);
			return false;
		}

		return true;
	}

	private boolean processStreams() throws IOException {
		LOG.info("Starting processStreams");
		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path inputFile = new Path(inputPath);
			if (!fs.exists(inputFile)) {
				LOG.error("File Not found" + inputFile);
				return false;
			}
			inputStream = fs.open(inputFile);

			Path outputFile = new Path(outputPath);
			if (fs.exists(outputFile)) {
				LOG.error("Replacing output file " + outputFile);
				fs.delete(outputFile, true);

			}
			outputStream = fs.create(outputFile);
			ByteBuffer inputBuffer = ByteBuffer.allocate(INPUT_BUFFER_SIZE);
			ByteBuffer outBuffer = ByteBuffer.allocate(INPUT_BUFFER_SIZE);
			int read;
			while ((read = inputStream.read(inputBuffer)) != -1) {
				inputBuffer.rewind();
				for (int i = 0; i < read; i++) {
					byte input=inputBuffer.get();					
					List<List<byte[]>> mapping = readStreamMapping(machineStreamList);
					for (List<byte[]> map : mapping) {
						input = map.get(i)[Util.toUnsigned(input)];
					}
					outBuffer.put(input);
				}
				outputStream.write(outBuffer.array());
				inputBuffer.clear();
				outBuffer.clear();
				
			}
			LOG.info("ProcessStream Done");
			return true;

		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return false;
		} finally {
			if (outputStream != null) {
				outputStream.close();
			}
			if (inputStream != null) {
				inputStream.close();
			}
			if (machineStreamList != null && !machineStreamList.isEmpty()) {
				for (FSDataInputStream stream : machineStreamList) {
					stream.close();
				}
			}
		}

	}

	private List<List<byte[]>> readStreamMapping(List<FSDataInputStream> streams) throws IOException {
		List<List<byte[]>> result = new ArrayList<>();
		for (FSDataInputStream stream : streams) {
			ByteBuffer buffer = ByteBuffer.allocate(STREAM_BUFFER_SIZE);
			buffer.clear();
			stream.read(buffer);
			List<byte[]> map = new ArrayList<>();
			buffer.rewind();
			for (int i = 0; i < buffer.limit() / 256; i++) {
				byte[] tmp = new byte[256];
				buffer.get(tmp);
				map.add(tmp);
			}
			result.add(map);
		}
		return result;

	}

	private List<FSDataInputStream> getMachineStreams(List<String> machineSequence) {
		try {
			List<FSDataInputStream> machineStreamList = new ArrayList<>();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			for (String machineId : machineSequence) {
				Path streamFile = Path.mergePaths(new Path(enigmaTempDir),
						new Path(Path.SEPARATOR + machineId + ".stream"));
				if (!fs.exists(streamFile)) {
					LOG.error("Stream Not found :" + streamFile);
					return null;
				}
				FSDataInputStream fin = fs.open(streamFile);
				machineStreamList.add(fin);
			}
			LOG.info("Opened machineStreamList of size:"+machineStreamList.size());
			return machineStreamList;
		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return null;
		}
	}

	private List<String> getMachineSequence() {
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path keyFile = new Path(keyPath);
			if (!fs.exists(keyFile)) {
				LOG.error("Key Not found :" + keyFile);
				throw new RuntimeException("Key Not found :" + keyFile);
			}
			FSDataInputStream fin = fs.open(keyFile);
			String keyJson = fin.readUTF();
			EnigmaKey key = new ObjectMapper().readValue(keyJson, EnigmaKey.class);
			LOG.info("Initial Machine Order:"+Arrays.toString(key.getMachineOrder().toArray()));
			return key.getMachineOrder();
		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return null;
		}
	}

}
