package com.tito.enigma.yarn.phase;

import java.util.ArrayList;
import java.util.List;

import com.tito.enigma.yarn.task.Task;

public abstract class Phase {

	private String id;
	private List<Task> taskList;
	
	public Phase(String id) {
		super();
		this.id = id;
		taskList=new ArrayList<>();
	}

	private PhaseManager phaseManager;
	private PhaseStatus phaseStatus;
	
	

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Task> getTaskList() {
		return taskList;
	}

	public void setTaskList(List<Task> taskList) {
		this.taskList = taskList;
	}

	public PhaseManager getPhaseManager() {
		return phaseManager;
	}

	public void setPhaseManager(PhaseManager phaseManager) {
		this.phaseManager = phaseManager;
	}

	public PhaseStatus getPhaseStatus() {
		return phaseStatus;
	}

	public void setPhaseStatus(PhaseStatus phaseStatus) {
		this.phaseStatus = phaseStatus;
	}

}
