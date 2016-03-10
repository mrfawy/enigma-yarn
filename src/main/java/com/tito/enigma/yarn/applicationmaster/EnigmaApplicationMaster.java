package com.tito.enigma.yarn.applicationmaster;

import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.enigma.yarn.phase.Phase;

public class EnigmaApplicationMaster extends ApplicationMaster {
	
	

	public boolean definePhases(){
		
		return true;
	}
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption("keyDir", true, "Dir to generate keys");		
		return opts;
	}

	@Override
	protected boolean init(CommandLine commandLine) {

		return true;
	}
	@Override
	protected void start() {
		
		
	}
}
