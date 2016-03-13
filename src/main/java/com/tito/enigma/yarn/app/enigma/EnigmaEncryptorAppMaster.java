package com.tito.enigma.yarn.app.enigma;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.tito.enigma.yarn.applicationmaster.ApplicationMaster;
import com.tito.enigma.yarn.phase.FixedTasksPhaseManager;
import com.tito.enigma.yarn.phase.Phase;
import com.tito.enigma.yarn.task.Task;
import com.tito.enigma.yarn.task.TaskContext;

public class EnigmaEncryptorAppMaster extends ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(EnigmaEncryptorAppMaster.class);

	private int enigmaCount;
	private String plainTextPath;
	private String cipherTextPath;
	private String enigmaTempDir;

	private List<String> machineIdList = new ArrayList<>();

	@Override
	public boolean init(CommandLine commandLine) {
		if (!commandLine.hasOption("enigmaCount")) {
			LOG.error("Missing enigmaCount");
			return false;
		} else {
			enigmaCount = Integer.parseInt(commandLine.getOptionValue("enigmaCount"));
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
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("enigmaCount", true, "Number of Engima Machines to encrypt with");
		opts.addOption("plainTextPath", true, "File to encrypt");
		opts.addOption("cipherTextPath", true, "Path to write encrypted file to");
		opts.addOption("enigmaTempDir", true, "Directory to write internal machine streams and generated Key");

	}

	@Override
	protected void registerPhases() {
		LOG.info("Registering Phases");

		List<Task> taskList = new ArrayList<>();
		for (int i = 0; i < enigmaCount; i++) {
			String machineId = "machine_" + i;
			machineIdList.add(machineId);
			TaskContext taskContext = new TaskContext(EnigmaGeneratorTasklet.class);
			taskContext.addArg("enigmaTempDir", enigmaTempDir);			
			taskContext.addArg("machineId", machineId);
			long length = getInputLength();
			if (length == -1) {
				LOG.error("Failed to get file length:" + plainTextPath);
				throw new RuntimeException("Failed to get file length:" + plainTextPath);
			}
			taskContext.addArg("length", String.valueOf(length));
			Task genTask = new Task("gen_" + i, taskContext);
			taskList.add(genTask);
		}
		FixedTasksPhaseManager phaseManager = new FixedTasksPhaseManager(this, taskList,
				new GeneratePhaseListener(this));
		Phase generatorPhase = new Phase("Generate Phase", phaseManager);
		registerPhase(generatorPhase);

		// combine phase
		List<Task> combineTasks = new ArrayList<>();
		TaskContext combineTaskContext = new TaskContext(EnigmaCombinerTasklet.class);
		combineTaskContext.addArg("inputPath", plainTextPath);
		combineTaskContext.addArg("outputPath", cipherTextPath);
		combineTaskContext.addArg("enigmaTempDir", enigmaTempDir);
		Task combineTask=new  Task("CombineTask", combineTaskContext);
		combineTasks.add(combineTask);
		FixedTasksPhaseManager combinePhaseManager = new FixedTasksPhaseManager(this, combineTasks,
				null);
		Phase combinePhase = new Phase("Combine", combinePhaseManager);
		registerPhase(combinePhase);
		
	}

	private long getInputLength() {

		try {
			Configuration conf = getConf();

			FileSystem fs = FileSystem.get(conf);
			Path input = new Path(plainTextPath);
			if (!fs.exists(input)) {
				LOG.error("plainTextPathh doesn't exist:" + plainTextPath);
				throw new RuntimeException("plainTextPathh doesn't exist:" + plainTextPath);
			}
			FileStatus fileStatus = fs.getFileStatus(input);
			return fileStatus.getLen();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
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
