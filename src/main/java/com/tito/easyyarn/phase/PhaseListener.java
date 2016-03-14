package com.tito.easyyarn.phase;

import com.tito.easyyarn.task.Task;

public interface PhaseListener {

	public void onPhaseStarted(Phase phase);
	public void onPhaseCompleted(Phase phase);	
	public void onPreTaskStart(Phase phase,Task task);
	public void onPreTaskReStart(Phase phase,Task task);
	public void onTasksCompletedSucessfully(Phase phase,Task task);
	public void onTasksFailed(Phase phase,Task task);
}
