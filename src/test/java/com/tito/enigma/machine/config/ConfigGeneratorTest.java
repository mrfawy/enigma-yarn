package com.tito.enigma.machine.config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.tito.enigma.machine.Util;

public class ConfigGeneratorTest {

	@Test
	public void testRotorConfig() {
		ConfigGenerator cf = new ConfigGenerator();
		RotorConfig rotorConfig = cf.generateRotorConfig();
		Set<Byte> notchSet = rotorConfig.getNotchSet();
		Assert.assertNotNull(notchSet);
		Assert.assertTrue(!notchSet.isEmpty());
		Assert.assertTrue(notchSet.size() <= 256);

		//unique values
		byte[] map = rotorConfig.getMap();
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
		Map<Byte, Byte> pc = cf.generatePlugBoardConfig();
		Assert.assertNotNull(pc);
		Assert.assertTrue(pc.size() <= 265 / 2);
		// non duplicate entires values or keys
		for (Byte k : pc.keySet()) {
			Assert.assertFalse(pc.containsKey(pc.get(k)));
		}

	}

	@Test
	public void testReflectorConfig() {
		ConfigGenerator cf = new ConfigGenerator();
		byte[] rc = cf.generateReflectorConfig();
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
