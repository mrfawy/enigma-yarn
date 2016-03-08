package com.tito.enigma.machine;

import java.util.Map;

import com.google.common.collect.BiMap;

public class PlugBoard implements SwitchIF {

	BiMap<Byte, Byte> map;

	public PlugBoard(Map<Byte, Byte> plugBoardConfig) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public byte[] signalIn(byte[] in) {
		byte[] result = new byte[256];
		for (byte i = 0; i < 256; i++) {
			if (map.containsKey(in[i])) {
				result[i] = map.get(i);
			} else {
				result[i] = i;
			}
		}
		return result;
	}

	@Override
	public byte[] reverseSignalIn(byte[] in) {
		byte[] result = new byte[256];
		for (byte i = 0; i < 256; i++) {
			if (map.containsValue(i)) {
				result[i] = map.inverse().get(in[i]);
			} else {
				result[i] = i;
			}
		}
		return result;	}

}
