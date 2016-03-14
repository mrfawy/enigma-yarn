package com.tito.easyyarn.task;

import java.util.HashMap;
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

	public void addArg(String key,String value){
		getArgs().put(key, value);
	}
	public Map<String, String> getArgs() {
		if(args==null){
			args=new HashMap<>();
		}
		return args;
	}

	

	public void addEnvVariable(String key,String value){
		getEnvVariables().put(key, value);
	}
	public Map<String, String> getEnvVariables() {
		if(envVariables==null){
			envVariables=new HashMap<>();
		}
		return envVariables;
	}

	

	public int getMaxMemory() {
		return maxMemory;
	}

	public void setMaxMemory(int maxMemory) {
		this.maxMemory = maxMemory;
	}

}
