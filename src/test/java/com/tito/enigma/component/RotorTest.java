package com.tito.enigma.component;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.avro.RotorConfig;
import com.tito.enigma.config.ConfigGenerator;

public class RotorTest {

	@Test
	public void testRotor() {
		ConfigGenerator cg = new ConfigGenerator();
		RotorConfig rc = cg.generateConfiguration(5, 5).getRotorConfigs().get(0);
		Rotor r = new Rotor(rc);
		byte[] input = Util.getArray(256);
		byte[] output = r.signalIn(input);
		Assert.assertTrue(output.length == input.length);
	}

}
