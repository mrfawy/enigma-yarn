package com.tito.enigma.machine;

public class Reflector implements SwitchIF {
	byte[] map;

	public Reflector(byte[] config) {
		this.map = config;
	}

	@Override
	public byte[] signalIn(byte[] in) {
		byte[] result = new byte[256];
		for (int i = 0; i < in.length; i++) {
			result[i] = map[Util.toUnsigned(in[i])];
		}
		return result;

	}

	@Override
	public byte[] reverseSignalIn(byte[] in) {
		return signalIn(in);
	}

}
