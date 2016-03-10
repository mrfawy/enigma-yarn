package com.tito.enigma.yarn.task;

import org.apache.hadoop.yarn.api.records.ContainerId;

public abstract class Task {

	private String id;
	private TaskStatus status;
	private ContainerId assignedContainerId;
	private TaskContext taskContext;
	private boolean restartable;
	private int restartMax;
	private int restartCount;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
	}



	public ContainerId getAssignedContainerId() {
		return assignedContainerId;
	}

	public void setAssignedContainerId(ContainerId assignedContainerId) {
		this.assignedContainerId = assignedContainerId;
	}

	public TaskContext getTaskContext() {
		return taskContext;
	}

	public void setTaskContext(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

	public boolean isRestartable() {
		return restartable;
	}

	public void setRestartable(boolean restartable) {
		this.restartable = restartable;
	}

	public int getRestartMax() {
		return restartMax;
	}

	public void setRestartMax(int restartMax) {
		this.restartMax = restartMax;
	}

	public int getRestartCount() {
		return restartCount;
	}

	public void setRestartCount(int restartCount) {
		this.restartCount = restartCount;
	}
	
	

}
