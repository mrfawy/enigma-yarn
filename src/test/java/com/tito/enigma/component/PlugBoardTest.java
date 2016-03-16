package com.tito.enigma.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.avro.WiringPair;
import com.tito.enigma.config.ConfigGenerator;

public class PlugBoardTest {

	@Test
	public void testPlugboard() {
		ConfigGenerator cg = new ConfigGenerator();
		List<WiringPair> config = cg.generateConfiguration(5, 5).getPlugBoardConfig();
		PlugBoard p = new PlugBoard(config);
		byte[] input = Util.getArray(256);
		byte[] output = p.signalIn(input);
		Map<Byte,Byte> configMap=new HashMap<>();
		for(WiringPair pair:config){
			configMap.put(pair.getFrom().byteValue(),pair.getTo().byteValue());
			configMap.put(pair.getTo().byteValue(),pair.getFrom().byteValue());
		}
		for (int i = 0; i < 256; i++) {
			if (configMap.containsKey((byte) input[i])) {
				Assert.assertTrue(output[i]==configMap.get(input[i]));
			}
		}

		Assert.assertTrue(output.length == input.length);
	}

}
