package com.tito.enigma.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tito.enigma.avro.WiringPair;

public class PlugBoard implements Switch {

	Map<Byte, Byte> map;

	public PlugBoard(List<WiringPair> WiringList) {
		map = new HashMap<>();
		for (WiringPair pair : WiringList) {
			map.put(pair.getFrom().byteValue(), pair.getTo().byteValue());
			map.put(pair.getTo().byteValue(), pair.getFrom().byteValue());
		}
	}

	@Override
	public byte[] signalIn(byte[] in) {
		byte[] result = new byte[256];
		for (int i = 0; i < 256; i++) {
			if (map.containsKey(in[i])) {
				result[i] = map.get(in[i]);
			} else {
				result[i] = (byte) in[i];
			}
		}
		return result;
	}

	@Override
	public byte[] reverseSignalIn(byte[] in) {

		return signalIn(in);
	}

}
