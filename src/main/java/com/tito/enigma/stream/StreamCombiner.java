package com.tito.enigma.stream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.tito.enigma.component.Util;

public class StreamCombiner {

	public ByteBuffer combine(ByteBuffer input, List<ByteBuffer> machineMapping,boolean reversed) {
		input.flip();
		int inputSize = input.limit();
		ByteBuffer output = ByteBuffer.allocate(inputSize);		
		for (ByteBuffer machineMap : machineMapping) {
			machineMap.flip();
		}
		List<ByteBuffer> machineMappingSequence=new ArrayList<>();
		//reverse sequence of machines when reversed
		if(reversed){
			for(int j=machineMapping.size()-1;j>=0;j--){
				machineMappingSequence.add(machineMapping.get(j));
			}
		}
		else{
			 machineMappingSequence=machineMapping;
		}

		for (int i = 0; i < inputSize; i++) {
			byte inputByte = input.get();			
			for (ByteBuffer machineMap : machineMappingSequence) {
				byte[] map = new byte[256];
				machineMap.get(map);
				if(reversed){
					map=Util.reverseByteMap(map);
				}
				inputByte = map[Util.toUnsigned(inputByte)];
			}
			output.put(inputByte);
		}
		return output;
	}
	
}
