package com.tito.enigma.component;

public interface Switch {

	public byte[] signalIn(byte[] in);
	public byte[] reverseSignalIn(byte[] in);
}
