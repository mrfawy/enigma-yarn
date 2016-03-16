package com.tito.sampleapp.enigma;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.easyyarn.phase.PhaseListener;
import com.tito.easyyarn.phase.PhaseManager;
import com.tito.easyyarn.task.Task;
import com.tito.easyyarn.task.TaskContext;
import com.tito.enigma.avro.EnigmaKey;
import com.tito.enigma.component.EnigmaKeyUtil;

public class StreamPhaseManager extends PhaseManager {
	private static final Log LOG = LogFactory.getLog(StreamPhaseManager.class);

	private EnigmaAppMaster appMaster;
	private String keyPath;
	private String tmpDir;
	private long length;
	private EnigmaKey enigmaKey;

	public StreamPhaseManager(EnigmaAppMaster appMaster, String keyPath, String tmpDir, long length,
			PhaseListener listener) {
		super(appMaster, listener);
		this.appMaster = appMaster;
		this.keyPath = keyPath;
		this.tmpDir = tmpDir;
		this.length = length;
	}

	@Override
	public boolean checkDependencies() {
		enigmaKey = EnigmaKeyUtil.loadKey(keyPath);
		if (enigmaKey == null) {
			LOG.info("Stream Phase dependency check:Failed to load key");
			return false;
		}
		LOG.info("Stream Phase dependency check:Passed");
		return true;
	}

	@Override
	public void stop() {
		

	}

	@Override
	public void defineTasks() {		
		for (CharSequence machineId : enigmaKey.getMachineOrder()) {
			TaskContext taskContext = new TaskContext(EnigmaStreamGeneratorTasklet.class);
			taskContext.addArg("enigmaTempDir", tmpDir);
			taskContext.addArg("keyPath", keyPath);
			taskContext.addArg("machineId", machineId.toString());
			taskContext.addArg("length", String.valueOf(length));
			Task genTask = new Task("stream_" + machineId, taskContext);
			RegisterTask(genTask);
		}

	}
}
