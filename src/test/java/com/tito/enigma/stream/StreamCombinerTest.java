package com.tito.enigma.stream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.component.Util;
import com.tito.enigma.config.Shuffler;

public class StreamCombinerTest {

	@Test
	public void test() {
		int inputSize = 10;
		int INPUT_BUFFER_SIZE=100*1024;
		ByteBuffer input = ByteBuffer.allocate(INPUT_BUFFER_SIZE);
		int machineCount=3;
		List<ByteBuffer> machineMaps = new ArrayList<>();
		for (int i = 0; i < machineCount; i++) {
			ByteBuffer map = ByteBuffer.allocate(256 * 100);
			for (int j = 0; j < inputSize; j++) {
				map.put(Shuffler.getShuffledArray(256));
			}
			machineMaps.add(map);
		}


		input.put(Util.getArray(inputSize));		
		StreamCombiner sc=new StreamCombiner();
		ByteBuffer output= sc.combine(input, machineMaps);
		Assert.assertEquals(output.position(), inputSize);
		//check single first element
		byte element=input.get(0);
		for(int i=0;i<machineCount;i++){
			element=machineMaps.get(i).get(Util.toUnsigned(element));			
		}
		Assert.assertTrue(element==output.get(0));
	}

}
