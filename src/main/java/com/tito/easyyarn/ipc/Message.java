package com.tito.easyyarn.ipc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {

	DestinationContext destinationContext;
	private String operation;
	private Map<String, String> operationArgs;

	public Message(DestinationContext destinationContext) {
		super();
		this.destinationContext = destinationContext;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public void addOperationArgument(String key, String value) {
		getOperationArgs().put(key, value);
	}

	public Map<String, String> getOperationArgs() {
		if (operationArgs == null) {
			operationArgs = new HashMap<>();
		}
		return operationArgs;
	}

	public DestinationContext getDestinationContext() {
		return destinationContext;
	}

	public void setDestinationContext(DestinationContext destinationContext) {
		this.destinationContext = destinationContext;
	}
	

}
