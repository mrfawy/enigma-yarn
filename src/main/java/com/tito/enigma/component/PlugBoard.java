package com.tito.enigma.component;

import java.util.Map;

import com.google.common.collect.HashBiMap;

public class PlugBoard implements Switch {

	Map<Byte, Byte> map;

	public PlugBoard(Map<Byte, Byte> plugBoardConfig) {
		map = HashBiMap.create(plugBoardConfig.size());
		for (Byte k : plugBoardConfig.keySet()) {
			map.put(k, plugBoardConfig.get(k));
			map.put(plugBoardConfig.get(k), k);
		}
	}

	@Override
	public byte[] signalIn(byte[] in) {
		byte[] result = new byte[256];
		for (int i = 0; i < 256; i++) {
			if (map.containsKey(in[i])) {
				result[i] = map.get(in[i]);
			} else {
				result[i] = (byte)in[i];
			}
		}
		return result;
	}

	@Override
	public byte[] reverseSignalIn(byte[] in) {

		return signalIn(in);
	}

}
