package com.tito.enigma.yarn.applicationmaster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface ApplicationMasterIF {	
	boolean init(CommandLine commandLine);
	void setupOptions(Options opts);	
}
