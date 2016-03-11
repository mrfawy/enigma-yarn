package com.tito.enigma.ipc;

public class DestinationContext {

	private DestinationType destinationType;
	private String destinationId;

	public DestinationContext(DestinationType destinationType, String destinationId) {
		super();
		this.destinationType = destinationType;
		this.destinationId = destinationId;
	}

	public DestinationType getDestinationType() {
		return destinationType;
	}

	public void setDestinationType(DestinationType destinationType) {
		this.destinationType = destinationType;
	}

	public String getDestinationId() {
		return destinationId;
	}

	public void setDestinationId(String destinationId) {
		this.destinationId = destinationId;
	}

}
