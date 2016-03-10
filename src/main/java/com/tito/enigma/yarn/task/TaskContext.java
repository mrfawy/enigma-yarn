package com.tito.enigma.yarn.task;

import java.util.Map;

public class TaskContext {

	private Class taskletClass;
	private Map<String, String> args;
	private Map<String, String> envVariables;
	private int maxMemory; // optional to set assigned container memory, use it
							// for larger tasks

	public TaskContext(Class taskletClass) {
		super();
		this.taskletClass = taskletClass;
	}

	public Class getTaskletClass() {
		return taskletClass;
	}

	public void setTaskletClass(Class taskletClass) {
		this.taskletClass = taskletClass;
	}

	public Map<String, String> getArgs() {
		return args;
	}

	public void setArgs(Map<String, String> args) {
		this.args = args;
	}

	public Map<String, String> getEnvVariables() {
		return envVariables;
	}

	public void setEnvVariables(Map<String, String> envVariables) {
		this.envVariables = envVariables;
	}

	public int getMaxMemory() {
		return maxMemory;
	}

	public void setMaxMemory(int maxMemory) {
		this.maxMemory = maxMemory;
	}

}
