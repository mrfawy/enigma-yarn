package com.tito.enigma.component;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.config.ConfigGenerator;

public class ReflectorTest {

	@Test
	public void testReflector() {
		byte[] input = Util.getArray(256);
		ConfigGenerator cg = new ConfigGenerator();
		Reflector r = new Reflector(cg.generateConfiguration(5, 5).getReflectorConfig());
		byte[] output = r.signalIn(input);		
		Assert.assertTrue(output.length == input.length);
		byte[] reverse=r.reverseSignalIn(output);
		Assert.assertArrayEquals(input, reverse);
	}

}
