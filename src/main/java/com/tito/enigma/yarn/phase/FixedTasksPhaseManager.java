package com.tito.enigma.yarn.phase;

import com.tito.enigma.yarn.applicationmaster.ApplicationMaster;
import com.tito.enigma.yarn.task.Task;

public class FixedTasksPhaseManager extends PhaseManager {

	public FixedTasksPhaseManager(ApplicationMaster appMaster, Phase phase) {
		super(appMaster, phase);

	}

	@Override
	public void defineTasks() {
		for (Task t : getPhase().getTaskList()) {
			RegisterTask(t);
		}

	}

	@Override
	public boolean checkDependencies() {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
