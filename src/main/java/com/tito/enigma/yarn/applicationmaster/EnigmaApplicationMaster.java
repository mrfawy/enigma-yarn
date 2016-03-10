package com.tito.enigma.yarn.applicationmaster;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.tito.enigma.yarn.phase.FixedTasksPhaseManager;
import com.tito.enigma.yarn.phase.Phase;
import com.tito.enigma.yarn.task.DummyTaskLet;
import com.tito.enigma.yarn.task.Task;
import com.tito.enigma.yarn.task.TaskContext;

public class EnigmaApplicationMaster extends ApplicationMaster {

	public void registerPhases() {

		Task t = new Task("task1", new TaskContext(DummyTaskLet.class));
		List<Task> fixedTasks = new ArrayList<>();
		fixedTasks.add(t);
		Phase phase1 = new Phase("phase 1", new FixedTasksPhaseManager(this, fixedTasks));
		registerPhase(phase1);

	}
	
	
	@Override
	public boolean init(CommandLine commandLine) {		
		return true;
	}

	@Override
	public void setupOptions(Options opts) {
		opts.addOption("keyDir", true, "Dir to generate keys");
		
	}

	
	

}
