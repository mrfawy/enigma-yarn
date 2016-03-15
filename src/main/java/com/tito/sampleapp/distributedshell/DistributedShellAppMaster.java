package com.tito.sampleapp.distributedshell;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tito.easyyarn.appmaster.ApplicationMaster;
import com.tito.easyyarn.phase.FixedTasksPhaseManager;
import com.tito.easyyarn.phase.Phase;
import com.tito.easyyarn.task.Task;
import com.tito.easyyarn.task.TaskContext;
import com.tito.sampleapp.helloworld.HelloWorldTasklet;

public class DistributedShellAppMaster extends ApplicationMaster {
	private static final Log LOG = LogFactory.getLog(DistributedShellAppMaster.class);

	private int n;
	private String command;

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("n", true, "Number of containers to run the command ");
		opts.addOption("command", true, "Shell command to run");

	}

	@Override
	public boolean init(CommandLine cliParser) {
		if (!cliParser.hasOption("command")) {
			LOG.error("Missing command");
			return false;
		}
		command = cliParser.getOptionValue("command");

		if (cliParser.hasOption("n")) {
			LOG.error("Missing n");
			return false;
		}
		try {
			n = Integer.parseInt(cliParser.getOptionValue("n"));
		} catch (NumberFormatException e) {
			LOG.error("Invalid Number:", e);
			return false;
		}
		return true;

	}

	@Override
	protected void registerPhases() {
		List<Task> fixedTasks = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			TaskContext taskContext = new TaskContext(ShellTasklet.class);
			taskContext.addArg("command", command);
			Task t = new Task("task" + i, taskContext);
			fixedTasks.add(t);
		}
		Phase phase1 = new Phase("phase 1", new FixedTasksPhaseManager(this, fixedTasks, null));
		registerPhase(phase1);

	}

}
