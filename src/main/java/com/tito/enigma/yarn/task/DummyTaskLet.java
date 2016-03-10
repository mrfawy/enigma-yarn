package com.tito.enigma.yarn.task;

public class DummyTaskLet extends Tasklet {

	@Override
	public void initInstance() {
		setInstance(this);
	}
	@Override
	public boolean start() {
		System.out.println("hello world");
		return true;
	}

}
