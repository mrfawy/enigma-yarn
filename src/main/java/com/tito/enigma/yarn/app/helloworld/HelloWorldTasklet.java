package com.tito.enigma.yarn.app.helloworld;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.enigma.yarn.task.Tasklet;

public class HelloWorldTasklet extends Tasklet{

	@Override
	public boolean init(CommandLine commandLine) {		
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean start() {
		System.out.println("Hello world from HelloWorldTasklet!");
		return true;
	}

}
