package com.tito.sampleapp.helloworld;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.easyyarn.appmaster.ApplicationMaster;
import com.tito.easyyarn.phase.FixedTasksPhaseManager;
import com.tito.easyyarn.phase.Phase;
import com.tito.easyyarn.task.Task;
import com.tito.easyyarn.task.TaskContext;

public class HelloWorldAppMaster extends ApplicationMaster {

	@Override
	public boolean init(CommandLine commandLine) {
		return true;
	}

	@Override
	public void setupOptions(Options opts) {

	}

	@Override
	protected void registerPhases() {
		for (int i = 0; i < 10; i++) {
			Task t = new Task("task" + i, new TaskContext(HelloWorldTasklet.class));
			List<Task> fixedTasks = new ArrayList<>();
			fixedTasks.add(t);
			Phase phase1 = new Phase("phase 1", new FixedTasksPhaseManager(this, fixedTasks,null));
			registerPhase(phase1);
		}

	}

}
