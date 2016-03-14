package com.tito.enigma.stream;

import java.nio.ByteBuffer;
import java.util.List;

import com.tito.enigma.component.Util;

public class StreamCombiner {

	public ByteBuffer combine(ByteBuffer input, List<ByteBuffer> machineMapping) {
		input.flip();
		int inputSize = input.limit();
		ByteBuffer output = ByteBuffer.allocate(inputSize);		
		for (ByteBuffer machineMap : machineMapping) {
			machineMap.flip();
		}

		for (int i = 0; i < inputSize; i++) {
			byte inputByte = input.get();
			for (ByteBuffer machineMap : machineMapping) {
				byte[] map = new byte[256];
				machineMap.get(map);
				inputByte = map[Util.toUnsigned(inputByte)];
			}
			output.put(inputByte);
		}
		return output;
	}
}
