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

public class EnigmaEncryptorAppMaster extends ApplicationMaster{
	private static final Log LOG = LogFactory.getLog(EnigmaEncryptorAppMaster.class);

	private int enigmaCount;
	private String keyDir;
	private String plainTextPath;
	private String cipherTextPath;
	private String tempStreamDir;
	
	@Override
	public boolean init(CommandLine commandLine) {
		if(!commandLine.hasOption("enigmaCount")){
			LOG.error("Missing enigmaCount");			
			return false;
		}
		else{
			enigmaCount=Integer.parseInt(commandLine.getOptionValue("enigmaCount"));
		}
		
		if(!commandLine.hasOption("keyDir")){
			LOG.error("Missing keyDir");			
			return false;
		}
		else{
			keyDir=commandLine.getOptionValue("keyDir");
		}

		if(!commandLine.hasOption("plainTextPath")){
			LOG.error("Missing plainTextPath");			
			return false;
		}
		else{
			plainTextPath=commandLine.getOptionValue("plainTextPath");
		}

		if(!commandLine.hasOption("cipherTextPath")){
			LOG.error("Missing cipherTextPath");			
			return false;
		}
		else{
			cipherTextPath=commandLine.getOptionValue("cipherTextPath");
		}
		if(!commandLine.hasOption("tempStreamDir")){
			LOG.error("Missing tempStreamDir");			
			return false;
		}
		else{
			tempStreamDir=commandLine.getOptionValue("tempStreamDir");
		}
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("enigmaCount",true,"Number of Engima Machines to encrypt with");
		opts.addOption("keyDir",true,"Directory to store key");
		opts.addOption("plainTextPath",true,"File to encrypt");
		opts.addOption("cipherTextPath",true,"Path to write encrypted file to");
		opts.addOption("tempStreamDir",true,"Directory to write internal machine streams");
		
	}

	@Override
	protected void registerPhases() {
		
		Phase generatorPhase=new Phase("Generate Phase");
		
		List<Task> taskList=new ArrayList<>();
		for(int i=0;i<enigmaCount;i++){
			
			TaskContext taskContext=new TaskContext(EnigmaGeneratorTasklet.class);
			taskContext.addArg("keyDir", keyDir);
			taskContext.addArg("tempStreamPath", keyDir);
			long length=getInputLength();
			if(length==-1){
				 LOG.error("Failed to get file length:"+plainTextPath);
	                throw new RuntimeException("Failed to get file length:"+plainTextPath);
			}
			taskContext.addArg("length", String.valueOf(length));
			Task genTask=new Task("gen_"+i, taskContext);
		}
		FixedTasksPhaseManager phaseManager = new FixedTasksPhaseManager(this, taskList);
		generatorPhase.setPhaseManager(phaseManager);
		registerPhase(generatorPhase);
		
	}
	private long getInputLength(){
		
        try {
        	Configuration conf = getConf();
            
            FileSystem fs = FileSystem.get(conf);
            Path input = new Path(plainTextPath);
            if (!fs.exists(input)) {
                LOG.error("plainTextPathh doesn't exist:"+plainTextPath);
                throw new RuntimeException("plainTextPathh doesn't exist:"+plainTextPath);
            }
			FileStatus fileStatus = fs.getFileStatus(input);
			return fileStatus.getLen();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	}



}
