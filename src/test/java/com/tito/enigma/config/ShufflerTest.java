package com.tito.enigma.config;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class ShufflerTest {

	@Test
	public void testGetShuffledArray() {
		byte[] ar = Shuffler.getShuffledArray(256);
		Assert.assertEquals(256, ar.length);
		Set<Byte> values = new HashSet<>();
		for (int b = 0; b < 256; b++) {
			Assert.assertNotNull(b);
			Assert.assertFalse(values.contains(b));
			values.add((byte) b);
		}

	}

}
