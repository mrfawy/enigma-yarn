package com.tito.enigma.machine;

public interface SwitchIF {

	public byte[] signalIn(byte[] in);
	public byte[] reverseSignalIn(byte[] in);
}
