package com.tito.enigma.config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.avro.MachineConfig;
import com.tito.enigma.avro.RotorConfig;
import com.tito.enigma.avro.WiringPair;
import com.tito.enigma.component.Util;

public class ConfigGeneratorTest {

	@Test
	public void testGenerateConfiguration() {
		ConfigGenerator cf = new ConfigGenerator();
		MachineConfig config = cf.generateConfiguration(5, 10);
		Assert.assertNotNull(config);
		Assert.assertNotNull(config.getPlugBoardConfig());
		Assert.assertFalse(config.getPlugBoardConfig().isEmpty());
		Assert.assertNotNull(config.getReflectorConfig());
		Assert.assertNotNull(config.getRotorConfigs());
		Assert.assertTrue(config.getRotorConfigs().size() >= 5 && config.getRotorConfigs().size() <= 10);
	}

	@Test
	public void testRotorConfig() {
		ConfigGenerator cf = new ConfigGenerator();
		RotorConfig rotorConfig = cf.generateRotorConfig();

		Set<Byte> notchSet = new HashSet<>();
		rotorConfig.getNotchSet();
		for (int i = 0; i < rotorConfig.getNotchSet().limit(); i++) {
			notchSet.add(rotorConfig.getNotchSet().get());
		}
		Assert.assertNotNull(notchSet);
		Assert.assertTrue(!notchSet.isEmpty());
		Assert.assertTrue(notchSet.size() <= 256);

		// unique values
		byte[] map = Util.toArray(rotorConfig.getMap());
		Assert.assertEquals(256, map.length);
		Set<Byte> values = new HashSet<>();
		for (Byte b : map) {
			Assert.assertFalse(values.contains(b));
			values.add(b);
		}

		Assert.assertNotNull(rotorConfig.getOffset());
	}

	@Test
	public void testPlugboardConfig() {
		ConfigGenerator cf = new ConfigGenerator();
		List<WiringPair> pc = cf.generatePlugBoardConfig();
		Assert.assertNotNull(pc);
		Assert.assertTrue(!pc.isEmpty());
		Assert.assertTrue(pc.size() <= 265 / 2);
		Map<Byte, Byte> configMap = new HashMap<>();
		for (WiringPair pair : pc) {
			configMap.put(pair.getFrom().byteValue(), pair.getTo().byteValue());			
		}
		// non duplicate entires values or keys
		for (Byte k : configMap.keySet()) {
			Assert.assertFalse(configMap.containsKey(configMap.get(k)));
		}

	}

	@Test
	public void testReflectorConfig() {
		ConfigGenerator cf = new ConfigGenerator();
		byte[] rc = Util.toArray(cf.generateReflectorConfig());
		Assert.assertNotNull(rc);
		Assert.assertEquals(256, rc.length);

		// check uniq values
		Set<Byte> values = new HashSet<>();
		for (Byte b : rc) {
			Assert.assertNotNull(b);
			Assert.assertFalse("Duplicate" + b + "in\n " + Arrays.toString(rc), values.contains(b));
			values.add(b);
		}

		// reflector for each i->j , exists j->i
		for (int i = 0; i < rc.length; i++) {

			Assert.assertEquals(Util.toUnsigned((byte) i),
					Util.toUnsigned(rc[Util.toUnsigned(rc[Util.toUnsigned((byte) i)])]));

		}

	}

}
