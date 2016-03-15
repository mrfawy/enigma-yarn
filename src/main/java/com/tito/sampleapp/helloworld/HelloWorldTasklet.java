package com.tito.sampleapp.helloworld;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.easyyarn.task.Tasklet;

public class HelloWorldTasklet extends Tasklet{

	@Override
	public boolean init(CommandLine commandLine) {		
		return true;
	}

	@Override
	public void setupOptions(Options opts) {	
		
	}

	@Override
	public boolean start() {
		System.out.println("Hello world from HelloWorldTasklet!");
		return true;
	}

}
