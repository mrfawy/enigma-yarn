package com.tito.easyyarn.task;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

public class TaskletReciever extends ReceiverAdapter {

	private Tasklet tasklet;

	public TaskletReciever(Tasklet tasklet) {
		super();
		this.tasklet = tasklet;
	}
	@Override
	public void receive(Message msg) {
		
		tasklet.recieveMessage(msg);
	}
}
