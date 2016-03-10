package com.tito.enigma.yarn.task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface TaskletIF {

	boolean init(CommandLine commandLine);
	void setupOptions(Options opts);

	boolean start();
	
}
