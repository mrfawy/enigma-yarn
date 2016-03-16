package com.tito.enigma.component;

import java.nio.ByteBuffer;

public class Reflector implements Switch {
	byte[] map;

	public Reflector(ByteBuffer config) {
		if(config.position()!=0){
			config.flip();
		}
		this.map = Util.toArray(config);
		
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
