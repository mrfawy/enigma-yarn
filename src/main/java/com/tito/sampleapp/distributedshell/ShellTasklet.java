package com.tito.sampleapp.distributedshell;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.easyyarn.task.Tasklet;

public class ShellTasklet extends Tasklet {
	private static final Log LOG = LogFactory.getLog(ShellTasklet.class);

	private String command;

	@Override
	public void setupOptions(Options options) {
		options.addOption("command", true, "Shell command to run");

	}

	@Override
	public boolean init(CommandLine cliParser) {
		if (!cliParser.hasOption("command")) {
			LOG.error("Missing command");
			return false;
		}
		command = cliParser.getOptionValue("command");
		return true;

	}

	@Override
	public boolean start() {
		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));

                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			LOG.error("Failed to execute command " ,e);
			return false;
		}

		LOG.info(output.toString());
		return true;
	}

}
