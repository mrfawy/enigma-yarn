package com.tito.enigma.yarn.phase;

import java.util.List;

import com.tito.enigma.yarn.applicationmaster.ApplicationMaster;
import com.tito.enigma.yarn.task.Task;

public class FixedTasksPhaseManager extends PhaseManager {

	private List<Task> taskList;

	public FixedTasksPhaseManager(ApplicationMaster appMaster, List<Task> taskList,PhaseListener listener) {
		super(appMaster,listener);
		this.taskList = taskList;

	}

	@Override
	public void defineTasks() {
		for (Task t : taskList) {
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
