package com.tito.sampleapp.enigma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.easyyarn.appmaster.ApplicationMaster;
import com.tito.easyyarn.phase.FixedTasksPhaseManager;
import com.tito.easyyarn.phase.Phase;
import com.tito.easyyarn.task.Task;
import com.tito.easyyarn.task.TaskContext;
import com.tito.enigma.config.EnigmaKey;

public class EnigmaAppMaster extends ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(EnigmaAppMaster.class);

	private int enigmaCount;
	private String plainTextPath;
	private String cipherTextPath;
	private String enigmaTempDir;
	private String keyPath;
	private boolean isEncrypt;

	private List<String> machineIdList = new ArrayList<>();

	@Override
	public boolean init(CommandLine commandLine) {
		if (!commandLine.hasOption("operation")) {
			LOG.error("Missing operation <E|D>");
			return false;
		}
		String operation = commandLine.getOptionValue("operation");
		if (!operation.equalsIgnoreCase("e") && !operation.equalsIgnoreCase("d")) {
			LOG.error("Invalid operation, please specify E or D");
			return false;
		}
		if (operation.toLowerCase().startsWith("e")) {
			isEncrypt = true;
		}

		if (isEncrypt) {
			if (!commandLine.hasOption("enigmaCount")) {
				LOG.error("Missing enigmaCount");
				return false;
			} else {
				enigmaCount = Integer.parseInt(commandLine.getOptionValue("enigmaCount"));
			}

		}

		if (!commandLine.hasOption("plainTextPath")) {
			LOG.error("Missing plainTextPath");
			return false;
		} else {
			plainTextPath = commandLine.getOptionValue("plainTextPath");
		}

		if (!commandLine.hasOption("cipherTextPath")) {
			LOG.error("Missing cipherTextPath");
			return false;
		} else {
			cipherTextPath = commandLine.getOptionValue("cipherTextPath");
		}
		if (!commandLine.hasOption("enigmaTempDir")) {
			LOG.error("Missing enigmaTempDir");
			return false;
		} else {
			enigmaTempDir = commandLine.getOptionValue("enigmaTempDir");
		}

		if (!commandLine.hasOption("keyPath")) {
			LOG.error("Missing keyPath");
			return false;
		} else {
			keyPath = commandLine.getOptionValue("keyPath");
		}
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("operation", true, "E|D for Encrypt or Decrypt");
		opts.addOption("plainTextPath", true, "File to encrypt");
		opts.addOption("cipherTextPath", true, "Path to write encrypted file to");
		opts.addOption("keyPath", true, "Path to EnigmaKey.key file");
		opts.addOption("enigmaTempDir", true, "Directory to write internal machine streams and generated Key");
		opts.addOption("enigmaCount", true, "Number of Engima Machines to encrypt with");

	}

	@Override
	protected void registerPhases() {
		if (isEncrypt) {
			registerEncryptPhases();
		} else {
			registerDecryptPhase();
		}

	}

	private long getInputLength(String filePath) {

		try {
			Configuration conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			Path input = new Path(filePath);
			if (!fs.exists(input)) {
				LOG.error("File doesn't exist:" + filePath);
				throw new RuntimeException("File doesn't exist:" + filePath);
			}
			FileStatus fileStatus = fs.getFileStatus(input);
			return fileStatus.getLen();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}

	private Phase createKeyGeneratorPhase(int machineCount,String keyPath) {

		List<Task> taskList = new ArrayList<>();
		for (int i = 0; i < machineCount; i++) {
			String machineId = "machine_" + i;
			machineIdList.add(machineId);
			TaskContext taskContext = new TaskContext(EnigmaKeyGeneratorTasklet.class);
			taskContext.addArg("enigmaTempDir", enigmaTempDir);
			taskContext.addArg("machineId", machineId);
			Task genTask = new Task("gen_" + i, taskContext);
			taskList.add(genTask);
		}
		FixedTasksPhaseManager phaseManager = new FixedTasksPhaseManager(this, taskList,
				new KeyGeneratorPhaseListener(this, keyPath));
		Phase keyGeneratorPhase = new Phase("Generate Phase", phaseManager);
		return keyGeneratorPhase;
	}

	private Phase createStreamGeneratorPhase(String keyPath, String tmpDir, long length) {		
		
		StreamPhaseManager phaseManager = new StreamPhaseManager(this,keyPath,tmpDir,length,null);
		Phase streamPhase = new Phase("Stream Phase", phaseManager);
		return streamPhase;

	}

	private Phase createCombinerPhase(String keyPath, String inputPath, String outputPath, String tmpDir) {
		List<Task> combineTasks = new ArrayList<>();
		TaskContext combineTaskContext = new TaskContext(EnigmaCombinerTasklet.class);
		combineTaskContext.addArg("keyPath", keyPath);
		combineTaskContext.addArg("enigmaTempDir", tmpDir);
		combineTaskContext.addArg("inputPath", inputPath);
		combineTaskContext.addArg("outputPath", outputPath);
		Task combineTask = new Task("CombineTask", combineTaskContext);
		combineTasks.add(combineTask);
		FixedTasksPhaseManager combinePhaseManager = new FixedTasksPhaseManager(this, combineTasks, null);
		Phase combinePhase = new Phase("Combine", combinePhaseManager);
		return combinePhase;

	}

	public EnigmaKey loadKey(String keyPath) {
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path keyFile = new Path(keyPath);
			if (!fs.exists(keyFile)) {
				LOG.error("Key not found ," + keyFile);
				return null;
			}
			FSDataInputStream fin = fs.open(keyFile);
			String keyJson = fin.readUTF();
			EnigmaKey enigmaKey = new ObjectMapper().readValue(keyJson, EnigmaKey.class);
			return enigmaKey;
		} catch (Exception ex) {
			LOG.error("Error={}", ex);
			return null;
		}
	}

	private void registerEncryptPhases() {
		LOG.info("Registering Encryption Phases");

		Phase keyGenPhase = createKeyGeneratorPhase(enigmaCount,keyPath);
		if (keyGenPhase == null) {
			LOG.error("Failed to create Key Generator phase");
			throw new RuntimeException("Failed to create Key Generator phase");
		}
		registerPhase(keyGenPhase);

		long length = getInputLength(plainTextPath);
		if (length == -1) {
			LOG.error("Failed to determine input length");
			throw new RuntimeException("Failed to determine input length");
		}
		Phase streamPhase = createStreamGeneratorPhase(keyPath, enigmaTempDir, length);
		if (streamPhase == null) {
			LOG.error("Failed to create Stream phase");
			throw new RuntimeException("Failed to create Stream phase");
		}
		registerPhase(streamPhase);

		Phase combinePhase = createCombinerPhase(keyPath, plainTextPath, cipherTextPath, enigmaTempDir);
		if (combinePhase == null) {
			LOG.error("Failed to create Combine phase");
			throw new RuntimeException("Failed to create Combine phase");
		}
		registerPhase(combinePhase);

	}

	private void registerDecryptPhase() {
		LOG.info("Registering Decryption Phases");

		long length = getInputLength(cipherTextPath);
		if (length == -1) {
			LOG.error("Failed to determine input length");
			throw new RuntimeException("Failed to determine input length");
		}
		Phase streamPhase = createStreamGeneratorPhase(keyPath, enigmaTempDir, length);
		if (streamPhase == null) {
			LOG.error("Failed to create Stream phase");
			throw new RuntimeException("Failed to create Stream phase");
		}
		registerPhase(streamPhase);

		Phase combinePhase = createCombinerPhase(keyPath, cipherTextPath, plainTextPath, enigmaTempDir);
		if (combinePhase == null) {
			LOG.error("Failed to create Combine phase");
			throw new RuntimeException("Failed to create Combine phase");
		}
		registerPhase(combinePhase);

	}

	public int getEnigmaCount() {
		return enigmaCount;
	}

	public void setEnigmaCount(int enigmaCount) {
		this.enigmaCount = enigmaCount;
	}

	public String getEnigmaTempDir() {
		return enigmaTempDir;
	}

	public void setEnigmaTempDir(String enigmaTempDir) {
		this.enigmaTempDir = enigmaTempDir;
	}

	public String getPlainTextPath() {
		return plainTextPath;
	}

	public void setPlainTextPath(String plainTextPath) {
		this.plainTextPath = plainTextPath;
	}

	public String getCipherTextPath() {
		return cipherTextPath;
	}

	public void setCipherTextPath(String cipherTextPath) {
		this.cipherTextPath = cipherTextPath;
	}

	public List<String> getMachineIdList() {
		return machineIdList;
	}

	public void setMachineIdList(List<String> machineIdList) {
		this.machineIdList = machineIdList;
	}

}