package com.tito.enigma.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.config.ConfigGenerator;
import com.tito.enigma.config.MachineConfig;



public class StreamGeneratorTest {

	@Test
	public void test() {
		ByteArrayOutputStream outputStream=new ByteArrayOutputStream();
		MachineConfig cf=new ConfigGenerator().generateConfiguration(2, 2);
		StreamGenerator sg=new StreamGenerator(cf);
		try {
			int n=10;
			sg.generateLength(10, outputStream);
			Assert.assertEquals(outputStream.toByteArray().length, 10*256);
		} catch (IOException e) {
			Assert.fail(e.getMessage());
			
		}
		
	}

}
