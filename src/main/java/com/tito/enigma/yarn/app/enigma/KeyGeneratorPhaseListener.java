package com.tito.enigma.yarn.app.enigma;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tito.enigma.machine.config.EnigmaKey;
import com.tito.enigma.machine.config.MachineConfig;
import com.tito.enigma.yarn.phase.Phase;
import com.tito.enigma.yarn.phase.PhaseListenerIF;
import com.tito.enigma.yarn.phase.PhaseStatus;
import com.tito.enigma.yarn.task.Task;

public class KeyGeneratorPhaseListener implements PhaseListenerIF {
	private static final Log LOG = LogFactory.getLog(KeyGeneratorPhaseListener.class);

	EnigmaAppMaster enigmaEncryptorAppMaster;
	private String outputKeyPath;
	public KeyGeneratorPhaseListener(EnigmaAppMaster enigmaEncryptorAppMaster,String outputKeyPath) {

		this.enigmaEncryptorAppMaster = enigmaEncryptorAppMaster;
		this.outputKeyPath=outputKeyPath;
	}

	@Override
	public void onPhaseStarted(Phase phase) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPhaseCompleted(Phase phase) {
		LOG.info("onPhaseCompleted starting");
		//generate one valid Key for all machines with combine instructions
		if (phase.getPhaseManager().hasCompletedSuccessfully()) {
			FSDataOutputStream fout = null;
			try {
				
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				Map<String,MachineConfig> machinConfigMap=new HashMap<>();
				
				for(String machineId:enigmaEncryptorAppMaster.getMachineIdList()){
					Path configFile = Path.mergePaths(new Path(enigmaEncryptorAppMaster.getEnigmaTempDir()), new Path(Path.SEPARATOR +"key"+Path.SEPARATOR+ machineId + ".key"));

					if (!fs.exists(configFile)) {
						LOG.error("File not exists config file" + configFile);
						throw new RuntimeException("Key file doesn't exist" + configFile);
					}
					FSDataInputStream fin = fs.open(configFile);
					String confJson = fin.readUTF();
					MachineConfig machineConfig = new ObjectMapper().readValue(confJson, MachineConfig.class);
					LOG.info("captured Machine Config for MachineId:"+machineId);
					machinConfigMap.put(machineId, machineConfig);
					fin.close();
				}
				EnigmaKey enigmaKey=new EnigmaKey();
				enigmaKey.setMachineConfig(machinConfigMap);
				enigmaKey.setMachineOrder(enigmaEncryptorAppMaster.getMachineIdList());
				Path keyFile = new Path(outputKeyPath);
				if (fs.exists(keyFile)) {
					LOG.info("Replacing key file" + keyFile);
					fs.delete(keyFile, true);
				}
				LOG.info("Creating EnigmaKey:"+keyFile);
				fout = fs.create(keyFile);
				String confJson = new ObjectMapper()
						.writeValueAsString(enigmaKey);
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

	@Override
	public void onPreTaskStart(Phase phase, Task task) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPreTaskReStart(Phase phase, Task task) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTasksCompletedSucessfully(Phase phase, Task task) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onTasksFailed(Phase phase, Task task) {
		// TODO Auto-generated method stub

	}

}
