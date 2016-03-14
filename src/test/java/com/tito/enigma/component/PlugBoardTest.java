package com.tito.enigma.component;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.config.ConfigGenerator;

public class PlugBoardTest {

	@Test
	public void testPlugboard() {
		ConfigGenerator cg = new ConfigGenerator();
		Map<Byte, Byte> config = cg.generateConfiguration(5, 5).getPlugBoardConfig();
		PlugBoard p = new PlugBoard(config);
		byte[] input = Util.getArray(256);
		byte[] output = p.signalIn(input);
		for (int i = 0; i < 256; i++) {
			if (config.containsKey((byte) input[i])) {
				Assert.assertTrue(output[i]==config.get(input[i]));
			}
		}

		Assert.assertTrue(output.length == input.length);
	}

}
