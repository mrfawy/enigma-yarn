package com.tito.sampleapp.enigma;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.tito.easyyarn.phase.Phase;
import com.tito.easyyarn.phase.PhaseListener;
import com.tito.easyyarn.task.Task;
import com.tito.enigma.avro.EnigmaKey;
import com.tito.enigma.avro.MachineConfig;

public class KeyGeneratorPhaseListener implements PhaseListener {
	private static final Log LOG = LogFactory.getLog(KeyGeneratorPhaseListener.class);

	EnigmaAppMaster enigmaEncryptorAppMaster;
	private String outputKeyPath;

	public KeyGeneratorPhaseListener(EnigmaAppMaster enigmaEncryptorAppMaster, String outputKeyPath) {

		this.enigmaEncryptorAppMaster = enigmaEncryptorAppMaster;
		this.outputKeyPath = outputKeyPath;
	}

	@Override
	public void onPhaseStarted(Phase phase) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onPhaseCompleted(Phase phase) {
		LOG.info("onPhaseCompleted starting");
		// generate one valid Key for all machines with combine instructions
		if (phase.getPhaseManager().hasCompletedSuccessfully()) {
		
			try {

				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(conf);
				Map<String, MachineConfig> machinConfigMap = new HashMap<>();

				for (String machineId : enigmaEncryptorAppMaster.getMachineIdList()) {
					Path configFile = new Path(
							enigmaEncryptorAppMaster.getEnigmaTempDir() + Path.SEPARATOR + machineId + ".spec");

					if (!fs.exists(configFile)) {
						LOG.error("File not exists config file" + configFile);
						throw new RuntimeException("Key file doesn't exist" + configFile);
					}
					SeekableInput input = new FsInput(configFile, conf);

					DatumReader<MachineConfig> configDatumReader = new SpecificDatumReader<MachineConfig>(MachineConfig.class);
					DataFileReader<MachineConfig> dataFileReader = new DataFileReader<MachineConfig>(input, configDatumReader);
					MachineConfig machineConfig = dataFileReader.next();										
					machinConfigMap.put(machineId, machineConfig);
					dataFileReader.close();
					
					
				}
				EnigmaKey enigmaKey = new EnigmaKey();
				enigmaKey.setMachineConfig(machinConfigMap);
				enigmaKey.setMachineOrder(enigmaEncryptorAppMaster.getMachineIdList());
				Path keyFile = new Path(outputKeyPath);
				if (fs.exists(keyFile)) {
					LOG.info("Replacing key file" + keyFile);
					fs.delete(keyFile, true);
				}
				LOG.info("Creating EnigmaKey:" + keyFile);
				FSDataOutputStream fout = fs.create(keyFile);

				DatumWriter<EnigmaKey> keyDatumWriter = new SpecificDatumWriter<EnigmaKey>(EnigmaKey.class);
				DataFileWriter<EnigmaKey> dataFileWriter = new DataFileWriter<EnigmaKey>(keyDatumWriter);
				dataFileWriter.create(enigmaKey.getSchema(), fout);
				dataFileWriter.append(enigmaKey);
				dataFileWriter.close();
				fout.close();

			} catch (Exception ex) {
				LOG.error("Error={}", ex);
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
