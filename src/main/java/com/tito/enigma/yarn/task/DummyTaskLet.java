package com.tito.enigma.yarn.task;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class DummyTaskLet extends Tasklet {

	
	@Override
	public boolean start() {
		System.out.println("hello world");
		return true;
	}

	@Override
	public boolean init(CommandLine commandLine) {
		
		return true;
	}

	@Override
	public void setupOptions(Options opts) {		
		
	}

}
